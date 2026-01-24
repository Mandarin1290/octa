# Liquidity Buckets

Overview
--------
Liquidity buckets classify assets by expected time-to-liquidate and inform redemption planning. The worst-case bucket governs redemption speed assumptions.

Buckets
-------
- `T0` — daily liquidity (<= 1 day)
- `T+5` — up to 5 days
- `T+20` — up to 20 days
- `Illiquid` — longer than 20 days or flagged illiquid

API
---
- `LiquidityBuckets(thresholds=None)`
- `classify_asset(AssetLiquidity)` — asset-level classification
- `aggregate_portfolio(positions)` — returns bucket weights and worst-case bucket
- `stress_adjusted_aggregate(positions, stress_factor)` — applies stress and recomputes buckets (downgrades liquidity)

Integration
-----------
- Use `aggregate_portfolio` to determine redemption planning and worst-case timeline.
- Stress tests should call `stress_adjusted_aggregate` with plausible stress multipliers to plan downgrades.
