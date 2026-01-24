# HF Cascade Runbook (Citadel‑Style Ablauf)

Ziel: Ein **klarer, reproduzierbarer, stage‑basierter** Ablauf, der Assets **nur weiterreicht, wenn sie PASS sind**.

Die Cascade ist bewusst streng und deterministisch aufgebaut:

1. **1D Global Gate** (Risk/Quality‑Vorselektion)
2. **1H Training+Gates** (nur PASS aus 1D)
3. **30m Training+Gates** (nur PASS aus 1H)
4. **5m Training+Gates** (nur PASS aus 30m)
5. **1m Training+Gates** (nur PASS aus 5m)

Jede Stage schreibt **Artefakte + Summary**, sodass der Run operational nachvollziehbar ist.

---

## 0) Voraussetzungen

- Raw‑Daten liegen als Parquets unter `raw/` (beliebige Asset‑Klassen), Namenskonvention: `SYMBOL_{TF}.parquet`.
- Optional:
  - FRED Macro Features: `FRED_API_KEY` als Env Var.
  - EDGAR Enrichment im Global Gate: Netzwerkzugang + sauberer User‑Agent.

---

## 1) HF‑Preset verwenden

Für „HF‑Niveau“ nutzt du das Preset `configs/cascade_hf.yaml`.

- Aktiviert Optuna Tuning (bounded) mit `optuna_trials=400`, Timeout, Early‑Stop.
- Aktiviert FRED‑Macro Features (wenn Key vorhanden; sonst Cache/Auto).
- Schreibt Debug‑Artefakte auch bei Gate‑FAIL (für Audit/Diagnose).

---

## 2) Starten (End‑to‑End)

Beispiel (voller Run inkl. 1D Global Gate, HF Preset):

```bash
RUN=$(date -u +%Y%m%dT%H%M%SZ)
python3 scripts/e2e_orchestrator.py \
  --run-id "$RUN" \
  --out-dir reports/cascade \
  --raw-root raw \
  --run-global-gate \
  --hf \
  --fast
```

Optional: EDGAR im 1D Global Gate aktivieren:

```bash
python3 scripts/e2e_orchestrator.py \
  --run-id "$RUN" \
  --raw-root raw \
  --run-global-gate \
  --gate-edgar \
  --gate-edgar-user-agent "OCTA/1.0 (research; contact=you@domain)" \
  --hf
```

---

## 3) Artefakte pro Run

Alles liegt unter:

- `reports/cascade/<RUN_ID>/`

Struktur:

- `global_gate_1d/` – Output des 1D Global Gates
- `1D/`
  - `pass_1d.txt`
  - `stage_summary.json`
- `1H/`, `30m/`, `5m/`, `1m/`
  - `pass_<tf>.txt` – Passlist der Stage
  - `missing_parquet.txt` – Symbole ohne Parquet für diese TF
  - `stage_summary.json` – Zählwerte/Laufzeit
  - `diagnostics/fast_reason_report/` – nur wenn 0 PASS und Diagnose aktiv
- `run_summary.json` – Gesamtsummary (Stages, Flags, Config)

---

## 4) Monitoring (Live im Terminal)

Wenn du den Run in einem separaten Terminal startest und live sehen willst:

- Wenn du in ein Logfile umleitest:

```bash
tail -f reports/cascade/<RUN_ID>/run.log
```

- Wenn du direkt in der Shell laufen lässt, siehst du die Stages sequenziell.

Operationaler Check (Pass‑Counts):

```bash
jq '.stages[] | {timeframe, n_pass, n_missing_parquet}' reports/cascade/<RUN_ID>/run_summary.json
```

---

## 5) Diagnose: 0 PASS in einer Stage

Wenn eine Stage 0 PASS produziert, läuft automatisch ein schneller Diagnose‑Sweep (fast reason report) über die Input‑Passlist.

Du findest die Gründe hier:

- `reports/cascade/<RUN_ID>/<TF>/diagnostics/fast_reason_report/`

---

## 6) Reproduzierbarkeit (HF‑Arbeitsweise)

- `--run-id` ist der Primär‑Key für Audit/Tracing.
- Jede Stage ist ein abgeschlossener Block (Input Passlist → Output Passlist + Summary).
- Die Pipeline ist „pass‑forward“: keine implizite Erweiterung der Universe während eines Runs.

---

## 8) Zielwerte: „locker genug“ ohne Risk-Floors zu verwässern

Angenommene Erwartungsbereiche (PASS‑Rate pro Stage):

- `1D`: 10–30%
- `1H`: 2–10%
- `30m`: 1–5%
- `5m`: 0.2–2%
- `1m`: 0.1–1%

Operationaler Check (automatisch):

```bash
python3 scripts/passrate_dashboard.py --run-dir reports/cascade/<RUN_ID>
```

Wenn PASS‑Raten zu niedrig sind, nutze das **Qualitäts‑Overlay**, das nur MIN‑Qualitätswerte lockert
(Sharpe/Sortino/ProfitFactor/Net‑to‑Gross/avg_net_trade_return) – aber **keine Risk‑MAXs** wie Drawdown/CVaR/Turnover:

```bash
export OCTA_GATE_OVERLAY_PATH=configs/gate_overlay_relax_quality.yaml
```

---

## 7) Safety

Standard ist `safe-mode` (fail‑closed / keine Live‑Arming‑Actions). Nur wenn du bewusst disarmst:

```bash
python3 scripts/e2e_orchestrator.py --no-safe-mode ...
```

Das ist absichtlich nicht der Standard.
