# Redemption Stress Engine

Overview
--------
`RedemptionStressEngine` models cash-redemption scenarios to estimate liquidation timelines, forced slippage, and capital loss. Outputs can feed sentinel/risk gates.

Scenarios
---------
- 10% AUM redemption
- 25% AUM redemption
- 10% redemption + correlated market stress (increased slippage)

Usage
-----
- Provide `portfolio_liquidity` mapping asset_id -> {`weight`, `liquidity_days`, `slippage_per_day`}.
- `run_scenarios(...)` returns deterministic `StressResult` objects for each scenario.
- `check_sentinels(results, slippage_threshold, loss_threshold)` returns boolean flags indicating breaches.

Audit
-----
- Emits `redemption_stress.result` per scenario and `redemption_stress.sentinel` when thresholds breached.
