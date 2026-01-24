# Scaling Impact Analyzer

Overview
--------
`ScalingImpactAnalyzer` estimates how a strategy's absolute returns degrade as Assets Under Management (AUM) increase. The model is intentionally conservative and deterministic.

Model
-----
- adjusted_return = base_return * exp(-beta * (aum/base_aum - 1))
- `beta` controls degradation speed (higher -> faster degradation).

API
---
- `simulate_scaling(historical_returns, base_aum, target_aums)` — returns expected returns and marginal return per AUM unit for each target AUM.
- `compute_break_even(historical_returns, base_aum, hurdle_rate)` — searches for AUM where return_rate (expected_return / aum) <= `hurdle_rate`.

Integration
-----------
- Feed outputs into allocator and soft/hard close logic to decide capacity, derisking, or closures.
