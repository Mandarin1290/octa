Live Capital Monitoring
=======================

Overview
--------
`LiveCapitalMonitor` tracks capital-aware operational metrics and emits auditable alerts when thresholds are breached.

Monitored Metrics
-----------------
- NAV drift: absolute fractional change vs a baseline NAV.
- Fee accrual: large accrued fee amounts within a monitoring window.
- Exposure vs capital: portfolio exposure as a fraction of available capital.

Alerting & Escalation
---------------------
- Alerts are recorded with evidence hashes (canonical JSON + SHA-256).
- Escalation policy: repeated triggers for the same metric escalate from `warning` to `critical` after a configurable count (default 3).

API
---
- `LiveCapitalMonitor(nav_drift_threshold=0.05, fee_accrual_threshold=10000.0, exposure_percent_threshold=0.5, escalate_count=3)`
- `record_nav(baseline_nav, current_nav)` — returns emitted alerts (if any).
- `record_fee_accrual(accrued_amount)` — returns alerts if accrual exceeds threshold.
- `record_exposure(exposure, capital)` — returns alerts if exposure percent exceeds threshold.
- `get_alerts(severity=None)` — list alerts; optionally filter by severity.

Usage Notes
-----------
- Integrate monitor into live telemetry ingestion and wire `get_alerts()` into your escalation/notification system.
- Alerts include deterministic `evidence_hash` for audit trails.
