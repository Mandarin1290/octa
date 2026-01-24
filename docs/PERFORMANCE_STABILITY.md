# Performance Stability

Detects increasing volatility and higher-moment instability in strategy returns.

Metrics
-------
- Rolling volatility ratio: recent_std / baseline_std.
- Rolling skew and excess kurtosis differences between recent and baseline windows.
- `stability_score` in 0..1 combining volatility and moment signals (higher = more unstable).

Integration
-----------
- Optionally calls `AlphaDecayDetector.detect_decay()` and `RegimeFitEngine` to augment the report.

Usage
-----
Call `PerformanceStabilityAnalyzer(baseline_window, recent_window).analyze(returns, alpha_detector=..., regime_engine=..., market_indicator=..., latest_market_value=...)`.
