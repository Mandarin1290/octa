# Liquidity Stress Testing

This document describes the deterministic stress testing module in `octa_sentinel.liquidity_stress`.

Scenarios
---------
- `StressScenario` is versioned (`name`, `version`) and contains `adv_shock_factor`, `spread_mult`, `vol_mult`, `gap_pct`.
- Examples: ADV shock 0.5 (50%), spread multiplied by 2x, vol 2x, gap 0.05 (5%).

Metrics
-------
- `time_to_liquidate_days` — estimated days to liquidate the computed max notional under shocked ADV.
- `expected_slippage_bps` — deterministic proxy: spread_bps + impact where impact = 10000*(size/daily_adv_notional).
- `forced_loss_estimate` — worst-case gap loss = size * gap_pct.

Audit
-----
- Each run writes `liquidity_stress.report` to the ledger with scenario version and metrics. If TTL exceeds configured threshold, a `gate_event` is appended.

Determinism
-----------
- No randomness; given identical inputs and scenario version the report is reproducible.
