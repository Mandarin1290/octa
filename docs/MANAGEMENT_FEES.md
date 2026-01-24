# Management Fees

Overview
--------
The `ManagementFeeEngine` accrues class‑specific management fees daily and reduces NAV by deducting fees from class cash balances.

Key behaviour
-------------
- Fees are annual rates applied pro rata: `fee = total_value * rate * (days/365)`.
- Fees reduce NAV (cash balance) and are auditable per class and as a daily summary.
- Use `accrue_daily(series)` to run a single‑day accrual over all classes.

Integration
-----------
- Ensure `ShareClass.apply_management_fee()` is used for per‑class accounting; the engine wraps that and emits a daily summary audit event.
