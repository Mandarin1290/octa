# OCTA Autonomous Paper Pipeline (Additive)

Goal: run end-to-end universe discovery → tiered gates → cascaded training → PASS-only artifacts → promote to PAPER, while keeping all existing OCTA functionality intact.

## Where things live today (high-level)
- Training core: `octa_training/core/pipeline.py` (`train_evaluate_package`)
- Training orchestration: `octa_training/run_train.py` and `scripts/train_multiframe_symbol.py`
- Artifact writing: `octa_training/core/packaging.py` (`save_tradeable_artifact`)
- Tradeable artifact IO: `octa_training/core/artifact_io.py`
- Audit ledger: `octa_ledger/*`
- Risk / sentinel / promotion gates: `octa_sentinel/*` (see `octa_sentinel/paper_gates.py`)
- IBKR sandbox contract (NOT real trading): `octa_vertex/broker/ibkr_contract.py`

## New autopilot modules
- `octa_ops/autopilot/universe.py`: `discover_universe()` → unified symbol list
- `octa_ops/autopilot/data_quality.py`: per symbol+timeframe data quality gate
- `octa_ops/autopilot/global_gate.py`: 1D eligibility gate (best-effort optional enrichers)
- `octa_ops/autopilot/cascade_train.py`: strict cascaded training runner using `train_evaluate_package`
- `octa_ops/autopilot/registry.py`: sqlite registry for runs/gates/artifacts/promotions/orders
- `octa_ops/autopilot/paper_runner.py`: paper runner (currently fail-closed unless real broker adapter wired)
- `scripts/octa_autopilot.py`: single entrypoint to run A→E (scan/gates/train/promote)

## Outputs
- `artifacts/runs/{run_id}/universe_candidates.json`
- `artifacts/runs/{run_id}/data_quality_matrix.csv`
- `artifacts/runs/{run_id}/data_quality_details.json`
- `artifacts/runs/{run_id}/global_gate_status.json`
- `artifacts/runs/{run_id}/gate_matrix.csv`
- `artifacts/registry.sqlite3`

## Fail-closed semantics
- If any required artifact is missing/invalid: no paper order and no live order.
- Each symbol/timeframe is isolated: failures do not crash the whole run.

## Determinism
- Training uses OCTA config seed (`cfg.seed`) and stable splits already enforced in core pipeline.
- Autopilot does not randomize symbol order; it scans and sorts deterministically.
