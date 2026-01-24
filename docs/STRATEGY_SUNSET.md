# Strategy Sunset & Retirement Engine

Purpose
-------
Provide a systematic, auditable process to retire strategies based on objective triggers.

Hard Rules
----------
- No emotional decisions: triggers must be programmatic and recorded.
- Retirement reversible only via governance: reinstatement requires explicit governance approval.

Triggers
--------
- `alpha_decay` (from alpha decay detector)
- `capacity_breach` (capacity erosion / breach)
- `regime_mismatch` (regime warning system)

API
---
- `SunsetEngine.add_strategy(name, capital)`
- `SunsetEngine.initiate_sunset(name, trigger, notes)`
- `SunsetEngine.perform_shutdown(name)`
- `SunsetEngine.reinstate(name, governance_approval=True, approver=...)`
- `SunsetEngine.get_audit()`

Notes
-----
- All actions are appended to `audit_log` with a canonical evidence hash for governance review.
- `perform_shutdown` reclaims capital into the engine (simulated) and marks the strategy as `retired`.
- Only a governance-approved reinstatement call can move a strategy from `retired` back to `active`.
# Strategy Sunset (Auto-Sunsetting)

Purpose
-------
Automated, irreversible retirement for strategies when confirmed by multiple independent parties.

Key rules
---------
- Sunset is irreversible once executed.
- Requires `required_confirmations` independent confirmations.
- Supports committee confirmation as one confirmer if configured.
- On sunset, engine requests safe unwind via allocator (`unwind`), blocks trading (`block_trading`/`suspend`) and raises sentinel gate level 3.

Usage
-----
Create `SunsetEngine(audit_fn=..., sentinel_api=..., allocator_api=..., committee_check=..., config=...)`.
- Use `record_trigger()` to record evidence.
- Use `confirm()` by independent parties; once confirmations >= required the engine executes sunset.
