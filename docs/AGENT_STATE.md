# OCTA – AGENT_STATE (Working Memory)

## Mission
Build a fully autonomous, fail-closed, hedge-fund-grade training → gating → paper → live pipeline.
Key focus right now: cascade training correctness + micro-timeframe robustness + tuning budget control.

## Non-negotiables
- No deletions of existing logic. Only add wrappers, config toggles, adapters, and new modules.
- Fail-closed behavior must remain (invalid metrics => FAIL/ERR, never silent pass).
- Strict cascade is intentional: only PASS promotes to next timeframe.
- Keep changes minimal and auditable (small diffs, clear logs, deterministic behavior).
- Prefer reproducibility over “magic”.

## Current Status (as of last session)
- Cascade mechanism confirmed to reach 1m (1D → 1H → 30m → 5m → 1m) using smoke runs.
- Fixed a critical crash: Pydantic MetricsSummary validation errors due to NaN/inf/out-of-range metrics.
- Implemented sanitization layer in:
  - octa_training/core/evaluation.py
    - helper `_safe_float`
    - sanitizes numeric metrics before constructing MetricsSummary
    - clamps max_drawdown into [0,10]
    - maps invalid to None (preserves fail-closed: gates treat None as FAIL)
- Smoke run evidence:
  - decisions created up to 1m in /tmp/octa_smoke_reports/cascade/<run_id>/A/<tf>/decision.json
  - 5m PASS after sanitization; 1m decision created but often FAIL due to missing metrics / insufficient activity.

## Key Known Issues
1) 1m often FAIL:
   - reasons seen: profit_factor None, missing_net_to_gross
   - likely due to 0 trades / insufficient exposure / microstructure noise / missing exposure metrics.
2) Optuna “~40 trials” stop:
   - root cause: tuning.timeout_sec=3600 in configs/cascade_hf.yaml + long trials
   - core tuner uses `study.optimize(n_trials=..., timeout=...)`
3) Strict cascade ignores continue_on_fail (by design). For smoke we used gates/slicing to validate mechanics.

## Important Files / Modules
- Cascade orchestration:
  - scripts/train_multiframe_symbol.py
  - octa_ops/autopilot/cascade_train.py
- Tuning:
  - octa_training/core/optuna_tuner.py
  - configs/cascade_hf.yaml
- Metrics / evaluation contract:
  - octa_training/core/evaluation.py
  - octa_training/core/metrics_contract.py

## Output / Debug Locations
- Smoke reports:
  - /tmp/octa_smoke_reports/cascade/<run_id>/<symbol>/<tf>/decision.json
- Multi-TF report example:
  - /tmp/octa_smoke_reports/A_multitf_smoke_<run_id>_<timestamp>.json

## Next Steps (priority order)
1) Fix tuning budget policy:
   - Make timeout optional (null disables timeout)
   - Prefer stopping by n_trials unless timeout explicitly set
   - Update configs/cascade_hf.yaml tuning.timeout_sec -> null or higher default
   - Add clear logging of stop reason, completed/pruned trials
   - Warn on legacy scripts/train_and_save.py hardcoding n_trials=20
2) Make 1m FAIL reasons actionable and improve trade/activity sufficiency:
   - Ensure net_to_gross & profit_factor are computed when possible
   - If 0 trades, emit explicit reason "zero_trades" instead of missing metrics
   - Add a “signal/activity sufficiency” precheck (trade_count, exposure, turnover)
   - Consider micro-timeframe signal adjustment (adaptive thresholds) WITHOUT loosening HF gates
3) Performance/RAM improvements for 5m/1m:
   - chunked parquet loading
   - caching
   - feature flags for heavy features


## Milestone
- Batch 5 verified green via `python -m compileall -q .` and `pytest -q tests/test_imports.py --maxfail=1 --disable-warnings` (1 passed; pytest_asyncio deprecation warning emitted)
- Added import smoke test: tests/test_imports.py
- Full test suite and lint/mypy still pending
