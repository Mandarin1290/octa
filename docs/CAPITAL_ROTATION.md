# Capital Rotation — Dynamic Capital Reallocation

Purpose
- Gradually rotate capital between alphas while respecting transaction costs, liquidity and risk gates.

Core rules
- Rotation is gradual: per-period moves are capped by `max_shift_fraction` of total capital.
- Transaction costs are applied at the declared `transaction_cost_rate` and reduce net received capital.
- Cooldown: after an allocation change, an alpha can be frozen for `cooldown_periods` periods.
- Risk gates: when active for an alpha, no rotation can move capital into or out of that alpha.

API
- `RotationEngine(transaction_cost_rate, max_shift_fraction, cooldown_periods)`: construct engine.
- `rotate_once(current_allocs, target_weights, total_capital, period, risk_gate_active)`: perform one rotation step. Returns `(new_allocs, moved_amounts, total_cost_paid)`.

Guidance
- Choose conservative `max_shift_fraction` (e.g., 0.05–0.2) to avoid abrupt jumps.
- For production, combine with `pre_risk` checks so risk gates automatically prevent rotation if thresholds are breached.
