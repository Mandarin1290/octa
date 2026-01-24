# Pre‑Risk Screen

This module runs strategy-level pre-risk checks before any capital exposure.

Checks performed
---------------
- Theoretical drawdown bounds (`max_drawdown`).
- Tail exposure (`tail_risk`) — fraction of returns below a threshold.
- Correlation with existing strategies (`corr`) — blocks if above a threshold.
- Liquidity feasibility (`liquidity_feasible`) — ensures available liquid capital covers expected turnover.

Use `run_pre_risk(...)` to get a deterministic result dict: `passed`, `reasons`, and `details` for audit and gate decisions.
