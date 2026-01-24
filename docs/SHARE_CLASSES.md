# Share Classes & Series

Overview
--------
This module models multiple share classes (series) under a fund. Each class maintains independent NAV, assets, cash and fee rules.

Key Concepts
------------
- `ShareClass` — per-class accounting: `shares_outstanding`, `cash_balance`, `assets`, fees, `high_water_mark`.
- `ShareClassSeries` — container for multiple `ShareClass` instances belonging to a fund.

Properties
----------
- Each class computes its own NAV via `compute_nav()`.
- Deposits / redemptions / allocations are applied to the specific class only; capital does not move between classes.
- Fee application methods: `apply_management_fee(period_days)` and `apply_performance_fee()`.

Audit
-----
- Mutating operations emit audit events via the configured `audit_fn`.
