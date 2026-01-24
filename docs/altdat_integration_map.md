# AltData Integration Map (OCTA)

Datum: 2026-01-13

## Ziel
AltData (FRED/EDGAR/optional News/Satellite) wird so integriert, dass:
- Zeitfenster ausschließlich aus der Trainings-Zeitreihe stammt (Bars-Index = Single Source of Truth).
- As-of Joins strikt *backward* sind (AltData darf nie in der Zukunft liegen).
- Bei Unsicherheit (Keys, Rate Limits, fehlende Timestamps) AltData *deaktiviert/skippt* wird (fail-closed für AltData, nicht für Training).
- Bestehende Signaturen und bestehendes Logging unverändert bleiben.

## Bestehende Hook-Punkte (ohne Signaturänderung)

### Feature Engineering
- Primär: `octa_training.core.features.build_features(raw, settings, asset_class, build_targets=True)`
  - Eingang: `raw` OHLCV `DataFrame` mit `DatetimeIndex`.
  - Zeitfensterquelle: `raw.index.min()/max()`.
  - Geeigneter, minimal-invasiver Hook: am Ende von `build_features()` optional `okta_altdat.sidecar.try_run(...)` aufrufen und zusätzliche Features per Index-Join mergen.
  - Default: AltData aus.

### Training Orchestrator
- `octa_training.core.pipeline.train_evaluate_package(...)` ruft `build_features(...)` auf.
  - Minimaler Kontext-Transport ohne Signaturänderung: über `settings` Objekt (z.B. `settings.symbol`, `settings.timezone`).
  - Bars-as-of Timestamp: Candle Close = `raw.index`.

## Neue Module (additiv)
- `okta_altdat/bootstrap_deps.py`: Import-Check + optional pip install (nur wenn `OKTA_ALTDATA_AUTO_INSTALL=1`).
- `okta_altdat/storage.py`: Pfade + DuckDB Schema + Metadatenablage.
- `okta_altdat/time_sync.py`: `TimeWindow`, `asof_join`, `validate_no_future_leakage`.
- `okta_altdat/connectors/*`: FRED/EDGAR (optional weitere Quellen).
- `okta_altdat/features/*`: Feature-Builder mit as-of merge und Feature-Store Persistenz.
- `okta_altdat/orchestrator.py`: Autonomer Ablauf pro Run.
- `okta_altdat/sidecar.py`: sichere Entry-Funktion für Feature-Pipeline.

## Konfiguration
- `config/altdat.yaml` (neu, ersetzt nichts)
- ENV:
  - `OKTA_ALTDATA_ENABLED=1` (override)
  - `OKTA_ALTDATA_STRICT=1` (Leakage-Verstöße => markiert/verwift)
  - `OKTA_ALTDATA_AUTO_INSTALL=1` (pip install missing deps)
  - `OKTA_ALTDATA_ROOT=...` (Speicherroot)
