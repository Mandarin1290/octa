Chaos Engineering — Controlled Failure Injection

Purpose

Provide a safe framework to inject randomized failures into non-live subsystems for resilience testing.

Principles

- Live trading must NEVER be affected: injectors refuse targets with `live=True` by default.
- Injections are deterministic when a `seed` is provided.
- All injections are logged; `recover_all()` reverses injected state.

Supported failures

- `restart`: simulate a process restart (calls `restart()` if present and marks `_restarted`).
- `delay`: mark the subsystem as delayed for a duration (sets `_delayed_for`).
- `partial_unavailable`: mark specific keys or endpoints as unavailable (`_unavailable_keys`).

Usage

- Create `FailureInjector(seed=...)` and call `random_inject(targets, failure_types, max_events)`.
- Recover with `recover_all(targets)` when done.

Safety

- Ensure `targets` only include non-live test instances.
- Review `injector.records` after runs to confirm actions taken.
