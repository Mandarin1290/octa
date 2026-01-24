# OCTA – Effizienz- und Speicheroptimierung (12. Jan 2026)

Ziel: Speicherverbrauch und Peak-RAM in OCTA reduzieren, ohne Funktionalität/Logik zu entfernen. Änderungen sind so gebaut, dass bestehende APIs/Outputs stabil bleiben.

## 1) Befunde (Hotspots)

**Größter RAM-Treiber im Codepfad Training/Gates**
- Parquet wird häufig vollständig geladen (`pyarrow.parquet.read_table(...).to_pandas()`), auch wenn nur wenige Spalten benötigt werden.
- In Sweeps (viele Symbole) addieren sich unnötige temporäre Objekte: Index-Diff-Berechnungen über `to_series()` erzeugen zusätzliche Series/Arrays.

**I/O & Artefakte (Disk, nicht RAM)**
- `reports/` und `artifacts/` enthalten sehr viele JSON/JSONL-Dateien. Das ist primär ein Disk-/I/O-Thema; für RAM relevant wird es erst, wenn komplette Reports gesammelt/geladen werden.

## 2) Umgesetzte Optimierungen

### 2.1 Parquet-Laden: Column Projection + geringerer Peak-Memory
Datei: `octa_training/core/io_parquet.py`

Änderungen:
- Beim Lesen via `pyarrow` wird jetzt (wenn möglich) **nur ein projiziertes Spaltenset** geladen:
  - Zeitspalte (aus Schema ermittelt)
  - `close` (required)
  - optional: `open/high/low/volume` + Delisting-Felder, falls vorhanden
- `pyarrow.parquet.read_table(..., memory_map=True, use_threads=True)` reduziert I/O-Overhead und Peak-RAM.
- `table.to_pandas(self_destruct=True, split_blocks=True)` (falls verfügbar) reduziert Spitzenverbrauch, weil Arrow-Buffers nach Konvertierung freigegeben werden können.
- High/Low-Sanity Checks vermeiden jetzt `pd.concat(...)` und arbeiten direkt über `df[["open","close"]]`.

Erwartete Wirkung:
- Deutlich weniger RAM bei großen Parquets (weniger Spalten, weniger temporäre Objekte).
- Bessere Skalierung für Sweeps/Batch-Training.

### 2.2 FX-G1 Spacing-Check: weniger temporäre Series
Datei: `octa_training/core/pipeline.py`

Änderung:
- Statt `df.index.to_series().diff()` wird nun `np.diff(df.index.asi8)` genutzt (nanosecond int64), anschließend Umrechnung in Sekunden.

Erwartete Wirkung:
- Reduziert Peak-Memory und CPU pro Symbol, gerade im Sweep-Pfad.

### 2.3 JSON-Serialization: orjson (optional) für geringere Overheads
Datei: `octa_training/core/device.py`

Änderung:
- `profile_to_json()` nutzt `orjson` wenn verfügbar, sonst stdlib `json`.

Erwartete Wirkung:
- Schneller und weniger Overhead beim Serialisieren von kleinen, häufigen Payloads.

## 3) Dependencies / Installation

Dateien:
- `requirements-runtime.txt`
- `requirements.txt`

Neu hinzugefügt:
- `psutil>=5.9.0` (bessere RAM/CPU Erkennung; schon optional im Code, jetzt offiziell in deps)
- `orjson>=3.10.0` (schnelles JSON)

Hinweis:
- `polars` ist als optional dokumentiert, aber **nicht erzwungen**, weil Wheel-Verfügbarkeit für Python 3.13 plattformabhängig sein kann.

## 4) Validierung

Ausgeführt (fokussiert auf Parquet/FX-Pfade):
- `pytest -q tests/test_fx_two_stage_strict_1h.py tests/test_fx_g1_recheck.py tests/test_smoke_test_parquet_suffix_fallback.py tests/test_imports.py`
- Ergebnis: **alle Tests grün**.

## 4.1 Optional: "Real Run" Memory-Sanity-Check aktivieren

Der Profiler ist **opt-in** und beeinflusst Default-Outputs nicht.

- Einmalig (CLI):
   - `OCTA_MEM_PROFILE=1 OCTA_MEM_PROFILE_TOP=25 .venv/bin/python -m octa_training.run_train --all --config configs/dev.yaml --safe-mode`
- Typische Interpretation der Felder im Log-Eintrag `msg="mem_profile"`:
   - `rss_mb`: tatsächlicher RSS des Prozesses
   - `traced_current_mb` / `traced_peak_mb`: tracemalloc-Heap (Python-Allocations), nicht inkl. aller nativen Allokationen
   - `top`: Top-Allokationen (Datei/Zeile + KB + Count)

## 5) Empfohlene nächste Schritte (optional, falls du noch mehr „maximal“ willst)

1. **Profiling im Real-Run**
   - Einmal ein repräsentativer Sweep mit `PYTHONMALLOC=malloc` + `tracemalloc`-Snapshots (nur diagnostisch) um weitere Peaks zu sehen.
2. **Report-/Artefakt-Policy**
   - Optional: große JSONL-Logs automatisch gzippen (nur wenn Consumer das akzeptieren).
3. **Weiteres Column-Pruning**
   - Falls bestimmte Pipelines garantiert nur `close` brauchen, können Call-Sites zukünftig explizit „required columns“ übergeben (API-Erweiterung).

