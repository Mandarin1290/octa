# Performance Fees (HWM-Based)

Overview
--------
Implements crystallization of performance fees using a High‑Water‑Mark (HWM) methodology. Fees are charged only on realized gains above the HWM and are applied per share class.

Rules
-----
- Fees only apply when class total > `high_water_mark`.
- Fee amount = (current_total - high_water_mark) * `performance_fee`.
- Fees reduce class cash balance (reducing NAV) and HWM is set to the post‑fee total.
- Engine prevents double charging by tracking the last crystallized total.

API
---
- `PerformanceFeeEngine(audit_fn=None)`
- `crystallize_fee(share_class)` — charges fee if conditions met, returns fee amount and emits `performance_fee.crystallized` audit event.
