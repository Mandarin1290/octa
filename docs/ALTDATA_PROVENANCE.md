# AltData Provenance

## Overview
AltData snapshots and features are written with provenance for auditability.

## Storage
- Cache: `octa/var/altdata/<source>/<YYYY-MM-DD>/...`
- Feature store (DuckDB or sqlite fallback): `octa/var/altdata/altdata.duckdb`

## Provenance Fields
- `run_id`
- `source`
- `asof`
- `fetched_at`
- `hash`
- `meta_json` (payload path, source metadata, config)

## Audit Procedure
1) Locate a run: `octa/var/altdata/altdata.duckdb`.
2) Query `altdata_provenance` for the run_id.
3) Recompute hash of cached payload JSON to verify integrity.
4) Cross-check source timestamps and config for reproducibility.
