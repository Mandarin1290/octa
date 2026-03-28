# File-by-File Change Overview

## New Files

- `configs/parquet_recycling_policy.yaml`
  - Conservative fail-closed policy surface for roots, thresholds, transforms and governance minima.
  - Benefit: removes magic numbers from code.
  - Risk: misconfigured thresholds can over-quarantine; default is intentionally conservative.

- `octa/core/data/recycling/__init__.py`
  - Package export for the new isolated framework.

- `octa/core/data/recycling/common.py`
  - Deterministic JSON, hashing, environment/git snapshots and stable file writing.
  - Benefit: reproducible evidence artifacts.

- `octa/core/data/recycling/models.py`
  - Typed dataclasses for inventory, classification, recycled artifacts, ROI and routing.
  - Benefit: explicit audit schema and easier tests.

- `octa/core/data/recycling/policy.py`
  - Policy loader with explicit configuration contract.
  - Benefit: governance-first configuration layer.

- `octa/core/data/recycling/evidence.py`
  - Append-only evidence-pack writer for run manifests, snapshots and hashes.
  - Benefit: deterministic audit trail per run.

- `octa/core/data/recycling/engine.py`
  - Core implementation for discovery, inventory, validation, classification, recycling, scoring and routing.
  - Benefit: single offline pipeline for parquet valorization.
  - Risk: inventory reads parquet files; still read-only and isolated from production flows.

- `octa/core/data/recycling/cli.py`
  - Deterministic multi-command CLI.
  - Benefit: operational entrypoints for inventory/full-run/evidence/quarantine reporting.

- `octa/cli.py`
  - Minimal CLI router exposing `parquet-recycling` without touching existing live/paper execution code paths.
  - Benefit: standard entrypoint via `python -m octa`.

- `tests/test_parquet_recycling_framework.py`
  - Focused tests for evidence completeness, fail-closed quarantine and deterministic inventory output.
  - Benefit: regression protection for governance-critical behavior.

- `docs/parquet_recycling_framework.md`
  - Architecture report plus operator guide.

- `docs/parquet_recycling_change_overview.md`
  - This file.

## Existing Files Left Unchanged By Design

- Existing shadow/paper/live execution entrypoints.
- Existing risk overlays and promotion gates.
- Existing training and model release governance controls.
- Existing raw parquet holdings.

That non-change is deliberate: the new framework is discovery/offline/governance-first and does not weaken production protection layers.
