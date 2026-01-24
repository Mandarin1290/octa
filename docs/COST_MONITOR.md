# Operational Cost & Friction Monitoring

Purpose
-------
Track operational costs (execution, financing, infrastructure, slippage delta) and surface real-time drift.

Design
------
- Maintain per-strategy append-only history of cost metrics.
- Compute long-window baseline and short-window recent averages.
- Emit `CostAlert` when relative increase exceeds configured `drift_threshold` and minimum alert score.
- Suggested actions may include `reduce_capacity` when increases are severe.
- All events produce canonical evidence hashes in `audit_log`.

API
---
- `CostMonitor.record_costs(date, strategy, execution, financing, infrastructure, slippage_delta)`
- `CostMonitor.set_capacity(strategy, capacity)`
- `CostMonitor.evaluate(strategy) -> Optional[CostAlert]`
- `CostMonitor.get_alerts()`
