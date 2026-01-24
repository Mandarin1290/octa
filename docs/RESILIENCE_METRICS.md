Resilience Metrics

Purpose

Define measurable, comparable resilience metrics for war‑game simulations that reflect survivability rather than profit.

Metrics

- `max_capital_loss`: maximum observed capital loss across runs (absolute units).
- `avg_recovery_time`: average time to recovery across runs (seconds).
- `total_incidents`: aggregate incident count observed.
- `gate_success_rate`: proportion of gate activations that succeeded.

Scoring

`ScoringEngine` computes metrics deterministically and produces a normalized resilience score (0..100) by combining:

- Loss component (higher is better when lower loss): 1/(1+loss)
- Recovery component: 1/(1+avg_recovery_time)
- Incident component: 1/(1+total_incidents)
- Gate component: gate success rate

Weights are configurable; default weights prioritize loss then other survivability factors.

Usage

Create `SimulationRun` objects describing each run and pass them to `ScoringEngine.compute_metrics` and `ScoringEngine.score`.
