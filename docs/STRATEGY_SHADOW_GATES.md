# Strategy Shadow Gates

Overview
--------
Deterministic, metric-based gates to promote a strategy from `SHADOW` to `LIVE`.

Required metrics (deterministic)
-------------------------------
- `runtime_days` >= default 14
- `deviation_vs_paper` <= default 0.05
- `projected_aum` <= `capacity_limit` * `capacity_buffer`
- `incidents` == 0
- `risk_budget_utilization` <= 1.0

API
---
- `octa_strategy.shadow_gates.ShadowGates.evaluate(metrics)` returns per-gate pass/fail and values.
- `promote_if_pass(lifecycle, metrics, doc)` performs documented transition to `LIVE` if all gates pass; otherwise signals `sentinel_api` and raises.
