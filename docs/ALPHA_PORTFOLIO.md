# Alpha Portfolio Construction

Purpose
- Build a diversified alpha portfolio from competing alphas while enforcing risk budgets and preventing single-alpha dominance.

Core rules
- Start from risk-adjusted utilities (optionally penalized for crowding).
- Limit each alpha to `max_per_alpha` of the portfolio to avoid dominance.
- Enforce behavioral diversification by capping cumulative weight of closely overlapping alphas (`behavior_threshold` / `max_behavior_share`).
- Enforce portfolio risk budget (sum weight * volatility) by scaling weights conservatively.

API
- `AlphaCandidate`: candidate spec including `base_utility`, `volatility`, and `exposure`.
- `optimize_weights(candidates, ...)` returns mapping `alpha_id -> weight` (weights sum to 1).

Notes
- This optimizer is a deterministic, heuristic engine suitable for governance and audit. For production, replace with a convex optimizer (e.g., CVXPY) for mean-variance or CVaR objectives.
