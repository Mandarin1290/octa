# Hard Close (Capacity Protection)

Overview
--------
The Hard Close engine enforces an absolute cap on AUM. When active, external inflows are rejected. Lifting a hard close requires committee approval and is audited.

Activation
----------
Triggered when any of the following occur:
- `aum_total >= absolute_cap`
- `persistent_alpha` degradation from scaling
- `regulatory_flag` or `liquidity_flag`

API
---
- `HardCloseEngine(absolute_cap, required_approvals=2, audit_fn=None)`
- `attach(aum_state)` — subscribes to `AUMState` snapshots and patches inflow to reject when active.
- `check_and_update(aum_total, persistent_alpha=False, regulatory_flag=False, liquidity_flag=False)` — evaluate triggers.
- `request_approval(approver_id)` — committee members record approval.
- `lift_if_approved()` — lifts hard close only if required approvals collected.

Audit
-----
Emits `capital.hard_close.activated`, `capital.hard_close.approval`, `capital.hard_close.overridden` and `capital.inflow.rejected` events to the audit stream.
