# Strategy Suspension & Kill Rules

Overview
--------
Defines automatic suspension and retirement rules for strategies. Rules are explicit, auditable and require committee approval for manual overrides.

Triggers
--------
- Persistent underperformance (drawdown exceeds threshold repeatedly)
- Structural risk violations (immediate suspend)
- Alpha decay detected repeatedly
- Repeated operational incidents

Behavior
--------
- Counters track persistence; thresholds configure when to suspend vs retire.
- On suspension/retire the system emits audit events and notifies `sentinel_api.set_gate`.
- Manual retirement requires explicit committee approval and is audited.
