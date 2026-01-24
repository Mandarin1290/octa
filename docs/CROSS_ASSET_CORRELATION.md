# Cross-Asset Correlation Regimes

Overview
--------
This component detects regime-dependent correlation behavior across assets and escalates when cross-asset contagion (correlation spikes) is observed.

Regimes
-------
- `risk-on`: low correlations (diversified market).
- `risk-off`: high correlations (contagion / flight-to-quality).
- `inflation-shock`: correlated movements driven by inflation surprises.

Mechanics
---------
- Compute pairwise Pearson correlation matrix from recent returns.
- Reduce matrix to mean absolute off-diagonal correlation as a summary statistic.
- Compare mean correlation to regime baseline (`risk-on`:0.10, `risk-off`:0.60, `inflation-shock`:0.50).
- If mean exceeds baseline by `spike_threshold` (default 0.15), trigger an escalation and propose an allocator compression factor ≤ 1.0.

Escalation
----------
- Escalation calls `sentinel_api.set_gate(2, reason)` and writes an audit event.
- Suggested compression is `max(0.1, baseline / mean_corr)` to reduce position sizes proportionally.

Integration
-----------
- Use `octa_core.cross_asset_corr.CrossAssetCorrelation.assess_and_escalate()` during periodic risk scans.

Notes
-----
- Heuristic approach; for production, use sliding-window PCA, regime-switching models and persistence checks.
