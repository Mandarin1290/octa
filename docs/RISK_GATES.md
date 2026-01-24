OCTA Risk Gates

Levels

- L0: Warning — informational, no execution change.
- L1: Derisk — reduce size, tighten limits; allow existing positions.
- L2: Freeze new orders — do not allow new orders; existing positions remain.
- L3: Flatten + Kill — aggressively flatten positions and disable trading.

Policy-driven

- Policies are defined in `octa_sentinel.policies.SentinelPolicy` and are fully versioned.
- `SentinelEngine.evaluate(inputs)` returns a `Decision` with `level` and `action`.

Audit-first

- Audit chain failures are authoritative; the engine consults `octa_ledger` and applies the configured `audit_failure_level`.

Examples

- Drawdown breach: if portfolio drawdown >= configured `max_portfolio_drawdown`, engine returns L2 (freeze new orders).
- Broker disconnect: configured level (default L2) triggers freeze or flatten depending on severity.
