Institutional Training Pipeline
================================

Hook Points (Integration Map)
-----------------------------
- Training per gate/timeframe: `octa_ops/autopilot/cascade_train.py` calls `octa_training/core/pipeline.train_evaluate_package`.
- Feature build + model training: `octa_training/core/features.build_features`, `octa_training/core/models.train_models`.
- Backtest metrics: `octa_training/core/evaluation.compute_equity_and_metrics`.
- Artifacts/run registry: `octa/core/orchestration/resources.py` and `octa/core/runtime/run_registry.py`.
- Paper/live order emission: `octa_ops/autopilot/paper_runner.py`.

New Modules Added
-----------------
- Validation: `octa/core/research/validation/walk_forward.py`, `octa/core/research/validation/purged_cv.py`
- Scoring & costs: `octa/core/execution/costs/model.py`, `octa/core/research/scoring/scorer.py`
- Risk overlay: `octa/core/risk/overlay.py`, `octa/core/risk/limits.py`
- Model release: `octa/core/governance/model_release.py`, `octa/core/governance/champion_challenger.py`
- Monte Carlo robustness: `octa/core/research/robustness/monte_carlo.py`
- Entry points: `octa/support/ops/run_institutional_train.py`, `octa/support/ops/run_institutional_smoke.py`

Flow
----
1) Cascade training via `run_cascade_training`.
2) Validation (WFV + Purged CV) generates artifacts under `octa/var/artifacts/validation/...`.
3) Cost-aware scoring uses net PnL and writes `octa/var/artifacts/scoring/...`.
4) Monte Carlo robustness stress tests write `octa/var/artifacts/robustness/...`.
5) Model release decision writes registry file `octa/var/registry/models/...` and audit.
6) Risk overlay gates paper/live orders in `octa_ops/autopilot/paper_runner.py`.

Fail-Closed Defaults
--------------------
- Missing validation/scoring/MC reports => model NOT released.
- Risk overlay errors => orders blocked.
- All audits written to `octa/var/audit/*` with deterministic seeds.
