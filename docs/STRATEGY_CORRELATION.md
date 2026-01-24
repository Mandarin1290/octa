# Strategy Correlation & Redundancy Detector

Overview
--------
Detects redundant or overly correlated strategies and proposes budget compression to avoid double-counting diversification.

Mechanics
---------
- Compute pairwise Pearson correlations from rolling returns.
- Redundancy score per strategy = mean absolute correlation to other strategies.
- If score ≥ `redundancy_threshold` (default 0.8), strategy is flagged as redundant.
- Budget compression applied as `new_budget = old_budget * max(0.1, 1 - score)`.

Integration
-----------
- Use `octa_strategy.correlation.StrategyCorrelation.assess(returns_by_strategy)` to get per-strategy redundancy report.
- Use `compress_budgets(budgets, report)` to adjust risk budgets before allocation.
