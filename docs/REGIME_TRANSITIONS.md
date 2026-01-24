# Regime Transitions — Conservative Response

Purpose
- Detect regime shifts and respond conservatively by dampening exposures, increasing uncertainty, and temporarily compressing risk.

Core rules
- When a regime change is detected, reduce exposure by `dampening_factor` (e.g., 0.3–0.7).
- Increase regime uncertainty by `uncertainty_increase` (capped at 1.0) to reflect added ambiguity.
- Apply `compression_periods` during which elevated caution/smaller exposures are maintained.
- Re-evaluate alpha scores using the higher uncertainty so downstream allocation engines act conservatively.

API
- `RegimeTransitionEngine(dampening_factor, uncertainty_increase, compression_periods)` — construct engine.
- `detect_transition(prev_regime, curr_regime)` — boolean.
- `handle_transition(prev, curr, current_exposure, regime_uncertainty)` — returns `(new_exposure, new_uncertainty, compression_remaining)`.
- `re_evaluate_score(...)` — wrapper to `score_alpha` that returns explainable score with updated uncertainty.

Notes
- For production, wire transitions to the `AuditChain` and combine with `RotationEngine` so changes propagate smoothly.
