# Soft Close (Growth Brake)

Overview
--------
The Soft Close engine prevents further external capital inflows while allowing internal compounding to continue. It is reversible and audited.

Triggers
--------
- Capacity utilization above `capacity_utilization` threshold.
- Slippage deterioration above `slippage_delta` threshold.
- Correlation crowding above `correlation_index` threshold.

API
---
- `SoftCloseEngine(thresholds=None, audit_fn=None)`
- `check_and_update(capacity_utilization, slippage_delta, correlation_index)` — evaluates triggers, activates soft close if any exceed thresholds.
- `attach(aum_state)` — attaches engine to an `AUMState` instance and blocks external inflows while active.
- `lift(reason)` — reverses soft close.

Effects
-------
- When active: external inflows via `AUMState.inflow(..., source='external')` are blocked and an audit event `capital.inflow.blocked` is emitted.
- Internal PnL and internal inflows remain allowed.

Integration
-----------
- Wire `audit_fn` to `AuditChain.append` for provenance.
- Call `check_and_update(...)` periodically from monitoring/health loops, passing capacity and market metrics.
