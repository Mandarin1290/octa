# Regime Fit

Overview
--------
Provides deterministic regime tagging and strategy↔regime compatibility metrics.

Design
------
- Regimes are tagged deterministically using tertile thresholds (`LOW`, `MID`, `HIGH`).
- Per-regime performance is aggregated (mean, std, count).
- Live compatibility score maps current regime performance to a 0..1 score relative to other regimes.
- Deterioration alerts flag regimes where performance is significantly below overall performance (z-test).

Usage
-----
Create `RegimeFitEngine()`, call `tag_regimes()` on a market indicator series, compute `performance_by_regime()` using strategy returns and those tags, then call `compatibility_score()` passing the latest market value and the computed per-regime stats.
