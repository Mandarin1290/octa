**TRANSACTION COSTS**

This document describes the deterministic transaction cost and slippage models.

Components
- Fixed fees: flat per trade `fixed_fees`.
- Spread cost: `half_spread * size` (price units * size).
- Market impact: square-root model: impact_pct = impact_coeff * sigma * sqrt(size / ADV).
- Time-of-day factor: deterministic multiplier (>1 means worse liquidity) applied to impact.

Usage
- Use `octa_vertex.costs.estimate_trade_cost(order, market, params)` for pre-trade forecasts.
- Use `octa_vertex.costs.realized_trade_cost(...)` for post-trade realized cost accounting.

Notes
- Models are deterministic and explainable. Parameters like `impact_coeff` and `fixed_fees` should be calibrated by ops.
- No optimistic assumptions: if ADV is zero, impact is zero but system should treat asset as illiquid upstream.
