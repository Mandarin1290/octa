# Internal Pricing (Cost of Capital)

Overview
--------
Internal Pricing charges strategies for the use of internal capital via an annualized `hurdle_rate`. Inefficient strategies (net negative after charge) are penalized by increasing their capital charge.

API
---
- `InternalPricing(hurdle_rate=0.10, penalty_multiplier=1.5, audit_fn=None)`
- `apply_charges(gross_returns, capital_used, period_days=365)` — returns `PricingResult` per strategy with `capital_charge` and `net_return`.

Integration
-----------
- Wire `audit_fn` to `AuditChain.append` to persist pricing events.
- Use `net_return` in allocator/ranking to penalize inefficient strategies.
