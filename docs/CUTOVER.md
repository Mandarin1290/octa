Production Cutover Protocol
===========================

Overview
--------
Defines a one-way cutover process for moving deployment/strategy state from `PAPER` to `SHADOW` and finally to `LIVE`.

Hard Rules
----------
- One-way transition: once in `LIVE`, the system cannot transition back. Attempts to revert raise `IrreversibleError`.
- No silent rollback: all transition attempts and final checks are recorded in the audit log.

Steps
-----
1. PAPER: system operating in simulation/paper mode.
2. SHADOW: system runs in a non-invasive shadow mode mirroring live flows but without executing trades.
3. LIVE: final irreversible state; capital is unlocked and the system transacts for real.

Final Checks
------------
Callers must provide a `check_fn` to `run_final_checks()` which returns a dict containing at least `{"ok": bool}`. Only if `ok` is `True` is transition to `LIVE` allowed.

Capital Unlock
--------------
Capital unlock is an explicit step performed when moving to `LIVE`. The `unlock_capital()` method records the action and sets the `capital_unlocked` flag.

Audit
-----
All actions are appended to an audit log with canonical JSON SHA-256 evidence hashes to make transitions auditable.

Usage Example
-------------
1. Create manager: `m = CutoverManager()`
2. Move to shadow: `m.transition_to_shadow()`
3. Run final checks: `m.run_final_checks(check_fn)` where `check_fn` returns `{"ok": True, ...}`
4. Move to live: `m.transition_to_live()` (irreversible)
