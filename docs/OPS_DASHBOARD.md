# Operations & Crisis Dashboard

Purpose: provide a deterministic, truthful operational view for oncall and exec.

Fields:

- `system_health`: boolean `ok` plus named checks (broker failures, stale feeds, degraded feeds, global halt).
- `active_incidents`: deterministic list of incidents (id, title, severity, timestamp).
- `broker_status`: per-broker health and last heartbeat; list of failed brokers.
- `feed_status`: per-instrument best available feed, degraded flag, and recovery flag.
- `trading_mode`: one of `normal`, `safe`, `halt` with precedence rules (halt > safe > normal).
- `recovery_progress`: whether in recovery, number of checkpoints, last checkpoint index.

Determinism:

- Ordering of lists is deterministic: incidents by (ts, id) and instruments/brokers sorted alphabetically.

Usage:

- Instantiate `OpsDashboard` with the core managers and call `snapshot()` to produce a JSON-serializable view.
