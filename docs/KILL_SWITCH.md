# Kill Switch

Purpose
-------
The kill-switch is a global, non-bypassable safety mechanism that can immediately freeze trading. It is intended to be authoritative: once TRIGGERED or LOCKED, execution paths must block until a sanctioned release occurs.

States
------
- `ARMED` — normal operation.
- `TRIGGERED` — kill-switch fired; trading blocked.
- `LOCKED` — administratively locked; requires sentinel or dual-operator manual release.

Rules
-----
- Only the `Sentinel` component may call `release_by_sentinel` to move the state back to `ARMED`.
- Manual release requires two operator confirmations (`manual_release`) and produces an auditable signed record.
- All state changes are audited using `kill_switch_change` audit events.

Enforcement
-----------
Critical execution paths must call into the kill-switch singleton via `get_kill_switch()` and check `get_state()` synchronously before proceeding. Implementations provided:

- `octa_sentinel.kill_switch.get_kill_switch()` — singleton accessor.
- `octa_vertex.kill_enforcement` (helpers) — convenience functions to enforce checks (used by pre-trade and shadow executor).
