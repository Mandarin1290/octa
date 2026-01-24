Long‑Run Stability Monitoring
=============================

Purpose
-------
Detect slow degradation in long‑running services without relying on PnL signals alone.

Monitored Metrics
-----------------
- Memory usage (MB)
- Latency (ms)
- Execution time variance (ms)
- Error‑rate (errors per minute)

Approach
--------
- Maintain sample histories and compute short‑term vs long‑term averages.
- Trigger when short/long ratio exceeds a configurable threshold (default 1.2).
- Escalate alerts from `warning` to `critical` after repeated triggers.

Usage
-----
- `LongRunStabilityMonitor()` — construct monitor and call `record_*` methods with metric samples.
- Call `evaluate()` periodically (e.g., every minute) to emit alerts.

Audit
-----
All samples, evaluations and alerts are appended to an audit log with canonical JSON SHA‑256 evidence hashes.
