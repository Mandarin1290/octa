# AltData Platform (okta_altdat)

## Aktivierung (safe-by-default)
Standard ist AltData deaktiviert.

Option A (Config):
- Editiere `config/altdat.yaml` und setze `enabled: true`.

Option B (ENV Override):
- `OKTA_ALTDATA_ENABLED=1`

## Wichtige ENV Variablen
- `FRED_API_KEY` (nur nötig wenn `sources.fred.enabled: true`)
- `OKTA_ALTDATA_STRICT=1` (Leakage-Verstöße => Features werden verworfen/NaN)
- `OKTA_ALTDATA_AUTO_INSTALL=1` (fehlende AltData-Deps werden via pip installiert)
- `OKTA_ALTDATA_ROOT=/pfad/zu/data/altdat` (Speicherroot, Default: `./data/altdat`)
- `OKTA_ALTDATA_CONFIG=/pfad/zu/config.yaml` (optional; Default: `config/altdat.yaml`)

## Wie läuft es technisch?
- `octa_training.core.pipeline` baut `eff_settings` und setzt optional `eff_settings.symbol`/`timezone`.
- `octa_training.core.features.build_features(...)` ruft am Ende optional `okta_altdat.sidecar.try_run(...)`.
- Sidecar:
  - liest Config
  - prüft Deps (und installiert optional)
  - baut AltData-Features via as-of join (strict backward)
  - shift(1) für zusätzliche Konservativität (wie die meisten Core-Features)
  - merged in die Feature-Matrix

## Outputs
Wenn aktiviert, schreibt AltData:
- Metadaten:
  - `data/altdat/meta/run_<run_id>.json`
  - `data/altdat/meta/features_<symbol>_<timeframe>_<run_id>.json`
- DuckDB:
  - `data/altdat/altdat.duckdb`
    - Tabellen: `meta_runs`, `meta_sources`, `fred_series`, `edgar_filings`, `feature_store`

## Beispiel: Training mit AltData
1) Setze `FRED_API_KEY`.
2) Aktiviere AltData: `OKTA_ALTDATA_ENABLED=1`.
3) Starte Training wie üblich (z.B. `octa_training/run_train.py --evaluate ...`).

Erwartung:
- Training läuft weiter, selbst wenn AltData-Deps fehlen (AltData deaktiviert sich fail-closed).
- Keine Änderung am existierenden Logging-Format.
