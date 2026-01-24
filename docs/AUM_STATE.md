# AUM State Engine

Overview
--------
`AUMState` is the canonical, time-versioned record of Assets Under Management (AUM).

Key design points
- AUM is first-class: all consumers should query `AUMState.get_current_total()` or subscribe to snapshots.
- Time-versioned: snapshots are recorded with ISO timestamps and stored in history.
- Auditable: every change and snapshot emits an audit event via the supplied `audit_fn`.
- Deterministic reconciliation: snapshots record both computed (internal+external) and reported (portfolio) totals and a `reconciled` flag.

API
---
- `AUMState(audit_fn=None, initial_internal=0.0, initial_external=0.0)`
- `set_internal_capital(amount, reason)` and `set_external_capital(amount, reason)` — set bookkeeping values and emit change events.
- `apply_pnl(pnl, reason)` — apply PnL (defaults to internal capital) and emit change event.
- `inflow(amount, source='external', reason)` / `outflow(...)` — capital movement hooks.
- `snapshot(portfolio_value)` — create a timestamped snapshot reconciled against `portfolio_value`.
- `subscribe(callback)` — callbacks receive `AUMSnapshot` on every snapshot.
- `get_current_total()`, `get_latest_snapshot()`, `history()`, `events()` — query helpers.

Integration
-----------
- Allocator, Capacity Engine and Risk Gates should read `get_current_total()` during decision making.
- Use `subscribe()` to get notified when AUM snapshots change and react (e.g. recompute capacity buckets).
- Provide `audit_fn` that forwards to `octa_ledger.AuditChain.append(...)` for persistent provenance.

Notes
-----
- This implementation keeps bookkeeping simple and deterministic. Downstream systems may implement more sophisticated ledger reconciliation if required.
