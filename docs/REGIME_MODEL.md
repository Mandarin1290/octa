# Regime Model

Overview
--------
This module provides a deterministic, explainable regime detector intended for use by the allocator
and sentinel. It is explicitly non-ML: rules are simple statistical heuristics that use only historical
data up to the time of detection (no lookahead).

Regimes
-------
- Volatility: `low`, `normal`, `high`
- Trend: `up`, `down`, `range`
- Correlation stress: `normal`, `elevated`

Algorithm (summary)
-------------------
- Volatility: compute short and long window realised volatility (population std of log-returns).
  - If short_vol > 1.5 * long_vol => `high`
  - If short_vol < 0.7 * long_vol => `low`
  - Else => `normal`
- Trend: perform an OLS slope on recent prices (trend window); normalise slope by mean price.
  - If slope_norm > threshold => `up`
  - If slope_norm < -threshold => `down`
  - Else => `range`
- Correlation stress: compute mean pairwise Pearson correlation of returns across the universe.
  - If mean_corr > 0.6 => `elevated` else `normal`

Explainability
---------------
The detector returns a `Regime` object containing labels and numeric `metrics` (volatility values,
raw slope, normalised slope and mean correlation) to support audits and deterministic decision-making.

Usage
-----
Instantiate `RegimeDetector()` and call `detect_at_index(prices, idx)` where `prices` is a mapping
from symbol to historical price list and `idx` is the current 0-based index (only data up to `idx` is used).

Configuration
-------------
All thresholds and window sizes are constructor parameters for `RegimeDetector`. The defaults are conservative
and suitable for common use, but can be tuned for the strategy universe.
