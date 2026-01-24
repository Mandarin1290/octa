OCTA Data Standard — Parquet Market Data

Overview

- Single-file-per-asset parquet files.
- Required columns: `timestamp` (UTC), `open`, `high`, `low`, `close`, `volume` (volume optional for FX only).
- Files must have monotonic increasing `timestamp` index, no duplicates, and no lookahead.
- No corporate-action placeholder columns; set `ca_provided: true` in the asset manifest if corporate actions are provided.

Manifest

- The `AssetManifest` contains `asset_id`, `symbol`, `asset_class` (EQUITY/ETF/FUTURE/FX/CRYPTO/BOND), `venue`, `currency`, `parquet_path`, and `ca_provided`.

Validation

- Implemented in `octa_stream.validate.ParquetValidator`.
- Schema checks types; rejects missing required columns or invalid types.
- Time checks: parseable to UTC, monotonic increasing, no duplicates.
- Sanity checks: non-negative prices, `high`/`low` relationships.
- Volume rules: required except for FX where null is allowed.

Lineage

- `octa_stream.lineage.parquet_content_hash` computes a SHA256 over file bytes plus parquet metadata to produce a stable content fingerprint used in model artifacts.

CLI

- `python -m octa_stream.validate --manifest <file>` produces eligibility and metadata report and exits with non-zero if ineligible.
