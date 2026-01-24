# Strategy Lifecycle (Idea → Paper → Shadow → Live → Retired)

Overview
--------
Defines a formal lifecycle for strategies: `IDEA`, `PAPER`, `SHADOW`, `LIVE`, `SUSPENDED`, `RETIRED`.

Rules
-----
- No state can be skipped: transitions must follow the allowed matrix.
- Every transition requires documentation and is audited (append-only history).
- Execution is only permitted in `LIVE`.
- Time-in-state tracking is available for monitoring and enforcement.

API
---
- `StrategyLifecycle(strategy_id, audit_fn)` — create lifecycle for a strategy.
- `transition_to(state, doc)` — perform a documented transition (raises on illegal).
- `time_in_state(state=None)` — seconds since last entered `state` (defaults to current).
- `can_execute()` / `assert_can_execute()` — execution guard.

Integration
-----------
Call `transition_to()` from governance workflows (e.g., `GoLiveCommittee`) and use `assert_can_execute()` in execution paths to prevent accidental runs.
