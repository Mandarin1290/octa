# Parquet Recycling & Data Valorization Framework

## Current State Forensics

- Raw parquet estates already exist under `raw/` with flat vendor-style roots such as `Stock_parquet`, `ETF_Parquet`, `FX_parquet`, `Futures_Parquet`, `Indices_parquet` and `Crypto_parquet`.
- Additional altdata material and metadata exist under `data/altdat/`.
- Existing OCTA building blocks already cover strict parquet discovery, canonical asset-class aliasing, append-only quarantine manifests, deterministic hashing, audit chains, evidence directories and fail-closed time-axis checks.
- Existing governance, promotion, execution and paper/live protection paths were left untouched. The new framework is offline and research-oriented only.

## Gap Analysis

- There was no unified, dataset-level parquet catalog that classifies unused parquet holdings into economic roles with explicit confidence, lineage and ROI-oriented routing.
- Existing checks were fragmented across universe preflight, stream validation, artifact quarantine and feature leakage tests.
- No dedicated audit pack existed for “recycle, classify, score, route” decisions on raw parquet inventories.

## Target Design

- `RAW ZONE`: read-only discovery from configured parquet roots.
- `CURATED ZONE`: allowed target route only, no raw mutation.
- `RECYCLED FEATURE ZONE`: deterministic compressed feature artifacts only.
- `MODEL-READY ZONE`: route can be recommended but never auto-promoted; existing OCTA promotion gates remain authoritative.
- `RISK / REGIME / MONITORING ZONE`: first-class destination for non-alpha utility.
- `SIMULATION / VALIDATION ZONE`: route for structurally sound market data.
- `QUARANTINE ZONE`: default fail-closed sink for unreadable, time-unsafe, low-confidence or unmapped datasets.

## Safety Decisions

- No raw parquet deletions.
- No changes to shadow, paper, live, execution, promotion or risk gates.
- Unknown asset mappings, unreadable parquet files and missing time axes fail closed into quarantine.
- Model-ready routing requires explicit policy thresholds and still does not bypass existing OCTA promotion/governance controls.
- `recommendation: review_for_governance_promotion` in the roi_report indicates a dataset met the numeric thresholds. It does NOT trigger any automated promotion. Any actual promotion must go through the existing OCTA governance and lifecycle gates.

## Evidence and Audit Scope

- Evidence is local to this framework only. Each run produces a flat SHA-256 manifest (`hashes.sha256`) covering all artifacts within the run directory.
- This framework does NOT emit events to the OCTA AuditChain (`AuditChain`, `EVENT_*`). Recycling decisions are not hash-chained and are not part of the main OCTA governance audit trail.
- `run_manifest.json` and `environment_snapshot.json` are runtime metadata (non-deterministic by design: timestamps, machine identity). The structural content artifacts (`dataset_catalog.json`, `classification_report.json`, `routing_report.json`) are deterministic given identical inputs and an identical `reference_time`.

## Determinism Scope

Deterministic (same inputs, same reference_time):
- `dataset_catalog.json`, `classification_report.json`, `validation_report.json`, `roi_report.json`, `routing_report.json`, `recycling_report.json`, `quarantine_report.json`

Non-deterministic by design (runtime metadata):
- `run_manifest.json` (`started_at` timestamp)
- `environment_snapshot.json` (machine platform, cwd)
- `git_snapshot.txt` (current repo state)

## Operator Guide

CLI entrypoint:

```bash
python -m octa parquet-recycling --policy configs/parquet_recycling_policy.yaml full-run
```

Supported subcommands:

- `inventory`
- `catalog`
- `validate`
- `classify`
- `recycle`
- `score`
- `route`
- `full-run`
- `evidence-report`
- `quarantine-report`

Outputs:

- Evidence pack: `octa/var/evidence/parquet_recycling/<run_id>/`
- Offline outputs: `artifacts/parquet_recycling/<run_id>/`

Required evidence artifacts per run:

- `run_manifest.json`
- `config_snapshot.json`
- `environment_snapshot.json`
- `git_snapshot.txt`
- `input_manifest.json`
- `dataset_catalog.json`
- `classification_report.json`
- `validation_report.json`
- `recycling_report.json`
- `routing_report.json`
- `roi_report.json`
- `quarantine_report.json`
- `summary.md`
- `hashes.sha256`

How to read reports:

- `dataset_catalog.json`: structural inventory, schema/time/quality/lineage baseline.
- `classification_report.json`: primary role, secondary roles, confidence and blocking flags.
- `roi_report.json`: economic utility and recommended zone.
- `routing_report.json`: final fail-closed route decision.
- `quarantine_report.json`: explicit reasons and evidence-bearing issues for blocked datasets.

How to handle quarantine:

- Fix the source mapping, timestamp semantics or data corruption upstream.
- Re-run inventory/validate/full-run.
- Do not inject quarantined datasets into training or model-ready paths.

How to extend safely:

- Adjust thresholds and allowed transforms only in `configs/parquet_recycling_policy.yaml`.
- Only transforms in the implemented set are executed: `zscore`, `rolling_delta`, `percentile`, `anomaly_flag`. Listing other names in the whitelist has no effect.
- Add new classification heuristics in the recycling module, not in live execution/training flows.
- Preserve deterministic JSON serialization and per-run evidence directory isolation.
