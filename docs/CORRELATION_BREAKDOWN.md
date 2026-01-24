**Correlation Breakdown Detection**

Overview
- This detector monitors rolling correlations across strategies/assets and raises an explainable stress score when diversification fails.

Algorithm
- Compute sample covariance over a rolling `window`.
- Apply deterministic shrinkage toward a scalar-identity prior to stabilise estimates.
- Convert shrunk covariance to correlation matrix.
- Compute metrics: average pairwise correlation, maximum pairwise correlation, and jump-rate (delta between last and previous window averages).
- Compose an explainable score in [0, 1] from normalized metrics. Larger values indicate correlation breakdown.

Outputs
- `score`: 0..1 composite stress score.
- `metrics`: raw values (`avg_pairwise`, `prev_avg`, `delta`, `max_pairwise`).
- `top_pairs`: list of top correlated pairs with their correlations.
- `recommended_compression`: suggested risk budget compression factor (0.1..1.0).

Integration guidance
- Feed the detector output into `CorrelationGates.evaluate_and_act` to have Sentinel and Allocator APIs set gates and apply downscaling.
- Prefer conservative thresholds for live trading (e.g. `avg_pairwise` 0.35, `max_pairwise` 0.7).

Determinism & Auditing
- All numerical computations are deterministic given identical inputs and seeds. The module emits explainable metrics that are safe to write to the ledger.
