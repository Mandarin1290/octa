# Regime‑Aware Alpha Scoring

This module scores alphas conditional on the current market regime while
preserving raw signal semantics and avoiding hindsight bias.

Key principles
--------------
- Regime‑awareness modifies attractiveness via a multiplicative regime
  compatibility multiplier — it does not change raw signal meaning.
- Regime uncertainty reduces the effective multiplier via a configurable
  uncertainty penalty weight.
- Scores are explainable: the returned object contains `base_score`,
  `regime_multiplier`, `uncertainty_modifier`, `composite_multiplier` and
  `final_score` so downstream reviewers can audit decisions.

API
---
- `score_alpha(signal, base_confidence, regime, regime_compatibility, regime_uncertainty=0.0, uncertainty_penalty_weight=0.5)`

Usage notes
-----------
- `regime_compatibility` should be defined by the research team (e.g. {'bull':1.2,'bear':0.6}).
- `regime_uncertainty` should come from a regime classifier producing a confidence score.
