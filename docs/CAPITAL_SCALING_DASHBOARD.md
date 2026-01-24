# Capital Scaling Dashboard

Overview
--------
Institutional view exposing deterministic, auditable capital scaling metrics used by risk and ops teams.

Outputs
-------
- `current_aum`: current reported AUM from `AUMState`.
- `capital_tier`: derived tier from `CapitalTierEngine`.
- `capacity_utilization`: deployed vs total scalable capacity (if allocator supplied).
- `soft_close_active` / `hard_close_active`: boolean flags from respective engines.
- `liquidity_buckets`: portfolio bucket weights mapping.
- `worst_bucket`: worst-case liquidity bucket for redemption planning.
- `scaling_break_even_aum` and `scaling_headroom`: break-even AUM and headroom computed by `ScalingImpactAnalyzer`.

Determinism & Audit
--------------------
- Inputs must be deterministic (explicit positions, audited AUM snapshots).
- Audit events are emitted by the underlying engines; the dashboard only aggregates deterministic outputs.

Usage
-----
- Instantiate with the canonical `AUMState`, `CapitalTierEngine`, `SoftCloseEngine`, `HardCloseEngine`, `LiquidityBuckets`, and `ScalingImpactAnalyzer` instances.
- Call `build(positions, expected_returns, base_aum, hurdle_rate)` to produce a dashboard dict.
