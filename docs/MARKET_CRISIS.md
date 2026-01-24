# Market Crash & Extreme Volatility Playbook

Purpose: provide a playbook and programmatic handler for extreme market stress. The system follows hard rules:

- Risk reduction prioritized over profit.
- Liquidity assumptions adjusted pessimistically.
- Human override allowed but always logged.

Triggers:

- Volatility spikes (e.g., realized or implied volatility breaches threshold).
- Correlation breakdowns (assets becoming highly correlated unexpectedly).
- Liquidity collapse (bid/ask depth evaporates; measured as liquidity metric falling below threshold).

Operator guidance:

- Automated mitigation attempts to reduce exposure conservatively.
- If automated mitigation fails (or cannot be executed), an automated kill-switch engages and prevents further automated trading.
- Human operators may override the kill-switch, but any override is logged with actor and timestamp; overrides should be short-lived and tracked.

Implementation notes:

- See `octa_ops/market_crisis.py` for `MarketCrisisManager`.
- The manager accepts market metrics with keys `volatility`, `correlation`, `liquidity` and applies a conservative `base_reduction` scaled up when liquidity is poor.
- All mitigation attempts and operator actions are appended to an `audit_log` list with UTC timestamps.

Recommended next steps:

- Wire `MarketCrisisManager` into `DetectionEngine` and `SafeModeManager` for enterprise workflows.
- Persist audit entries in an append-only store for post-mortem review.
