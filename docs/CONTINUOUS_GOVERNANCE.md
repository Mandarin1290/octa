# Continuous Governance Review Loop

Purpose
-------
Provide always-on governance cycles that emit auditable review records.

Cycles
------
- daily risk review (`daily_risk`)
- weekly strategy review (`weekly_strategy`)
- monthly governance committee (`monthly_committee`)

Behavior
--------
- All reviews are append-only in `audit_log` with a canonical evidence hash.
- Manual triggers available for each cycle; scheduler `run_scheduled(now_iso)` will run due cycles.

API
---
- `ContinuousReviewLoop.trigger_daily_risk_review(participants, notes)`
- `ContinuousReviewLoop.trigger_weekly_strategy_review(participants, notes)`
- `ContinuousReviewLoop.trigger_monthly_committee(participants, notes)`
- `ContinuousReviewLoop.run_scheduled(now_iso)`
- `ContinuousReviewLoop.get_audit()`

Notes
-----
- Use timezone-aware ISO timestamps.
- The scheduler uses 1 day / 7 day / 28 day thresholds for due checks.
