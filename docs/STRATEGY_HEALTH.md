# Strategy Health Score

Purpose
-------
Aggregate strategy diagnostics into a single, explainable health score in [0,1] where 1.0 is healthy.

Design
------
- Inputs: alpha decay, regime fit, stability, drawdown profile, risk utilization.
- Each input is mapped to a component health in [0,1] (1 = healthy). Example:
  - `alpha_decay.decay_score` -> `alpha_health = 1 - decay_score`.
  - `regime_fit.compatibility_score` -> `regime_health = compatibility_score`.
  - `stability.stability_score` -> `stability_health = 1 - stability_score`.
  - `drawdown.profile.classification` -> small penalty mapping.
  - `risk_util` -> `risk_health = 1 - min(1, risk_util)`.
- Weights are configurable but capped (default `max_contribution=0.4`) so no single metric dominates; weights are renormalized after capping.

Output
------
`HealthScorer.score(...)` returns a `HealthReport` with:
- `score`: final health in [0,1]
- `components`: per-component healths
- `contributions`: weighted contributions
- `explain`: raw inputs and the normalized weights, for auditability.

Usage
-----
Create `HealthScorer()` and call `.score(...)` passing the diagnostic reports. Use `explain` for human review and audit logging.
