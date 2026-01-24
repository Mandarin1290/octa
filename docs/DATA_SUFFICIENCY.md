# Data Sufficiency & Power Analysis

This module provides utilities to estimate required sample sizes, validate
regime coverage, and compute confidence intervals to ensure backtests are not
run on underpowered datasets.

Functions
---------
- `estimate_sample_size(effect_size, sigma, alpha=0.05, power=0.8)` — returns
  the minimal sample size (one-sample normal approximation) required to detect
  `effect_size` at given `alpha` and `power`.
- `confidence_interval(mean, std, n, alpha=0.05)` — two-sided CI for the mean.
- `validate_regime_coverage(regimes, required, min_fraction)` — checks that
  required regimes are present and meet minimal fraction coverage.
- `is_data_sufficient(...)` — composite check returning reasons and details.

Hard rules
----------
- No backtests on underpowered datasets: use `is_data_sufficient(...)` with
  `effect_size` and `power` to enforce minimum sample size.
