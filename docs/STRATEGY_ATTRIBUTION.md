# Strategy Attribution (Ex-Ante vs Ex-Post)

Overview
--------
Record ex‑ante expectations for strategies and compare ex‑post realized performance against them. Persistent deviations trigger a review/escalation.

API
---
- `AttributionEngine.record_expectation(strategy_id, expected_return, expected_vol)` — store expectation (versioned).
- `AttributionEngine.record_realized(strategy_id, realized_return, realized_vol)` — append realized event; evaluates deviation and triggers review if necessary.
- `AttributionEngine.deviation_metrics(strategy_id)` — returns summary of expected vs realized and reconciliation flag.
- `AttributionEngine.requires_review(strategy_id)` — boolean if last realized deviates beyond threshold.

Notes
-----
- `AttributionEngine` emits audit events and signals `sentinel_api.set_gate(2, reason)` on significant deviations.
- Expectations and realized events are kept in-memory; for persistence, wire `audit_fn` to `octa_ledger` append-only storage.
