# Auto Derisk

Overview
--------
Automated, reversible de-risking layer that gradually reduces strategy exposure based on a health score.

Behavior
--------
- Computes a derisk `factor` in [min_factor,1] from `health_score` and strategy scaling.
- Enforces a cooldown between actions.
- Tracks effectiveness (required fractional exposure reduction). If ineffective after `max_attempts`, escalates to suspension (audited).
- All actions are audited via `audit_fn` and can call `sentinel_api.set_gate` and `allocator_api` methods.

Integration
-----------
Call `AutoDerisk.process(strategy_id, health_score, current_exposure)` periodically from the monitoring loop. Register strategies via `register_strategy()` to set per-strategy scaling.
