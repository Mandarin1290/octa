War-Game Command Dashboard

Purpose

Provide centralized control and review for war-game simulations. Dashboard responsibilities:

- Track active simulations separately from finalized runs.
- Store finalized results immutably (hash snapshot).
- Aggregate resilience scores and surface uncovered weaknesses with remediation links.

Features

- `active_simulations()` returns currently running simulation IDs.
- `finalize_simulation(sim_id, run)` records immutable outcome and removes the simulation from active list.
- `resilience_score()` aggregates finalized runs with `ScoringEngine`.
- `uncovered_weaknesses()` returns simulations with gate failures or excessive loss and provides remediation links.

Usage

- Use `start_simulation(sim_id, metadata)` before running a war-game.
- On completion, call `finalize_simulation(sim_id, SimulationRun(...))` to record a run.
- Use `resilience_score()` and `uncovered_weaknesses()` for postmortem and reports.
