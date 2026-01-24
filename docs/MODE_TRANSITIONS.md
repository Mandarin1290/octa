# Mode Transitions (DEV → PAPER → SHADOW → LIVE)

Overview
--------
Mode transitions are strictly ordered and guarded. `ModeManager` enforces the state machine and records immutable audited events. Live enablement is a one-way gate except for emergency reversion requiring the kill-switch.

Rules
-----
- Transitions allowed: `DEV` → `PAPER` → `SHADOW` → `LIVE`.
- `enable_live` requires: checklist pass, committee approval (matured), and dual operator confirmation (both operators must be `EMERGENCY`).
- Reverting from `LIVE` requires the KillSwitch to be `TRIGGERED` or `LOCKED` and an incident; this action is audited.
- Every audit event includes the current mode in its payload.

Files
-----
- `octa_fabric/mode.py` — `ModeManager` implementation.
