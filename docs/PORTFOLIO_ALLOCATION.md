**PORTFOLIO ALLOCATION**

This document describes the allocator used for multi-strategy, risk-budgeted portfolio construction.

Algorithm summary
- Each `StrategyOutput` is scaled by its explicit `risk_budget` (a multiplier 0..1).
- Volatility targeting: exposures are scaled inversely by recent asset volatility (higher vol => smaller exposure).
- Correlation-aware scaling: exposures are downweighted by (1 + avg_abs_corr) to reduce concentration risk.
- Gross exposure cap: total absolute exposures are scaled to fit `gross_cap`.
- Net exposure cap per asset class: exposures for assets in a class are scaled to meet `net_cap`.

Attribution
- The allocator returns per-strategy attribution showing each strategy's contribution to final targets.

Usage
- Call `allocate()` with `strategy_results` as list of `(strategy_id, StrategyOutput)`, `current_portfolio`, `risk_budgets`, `prices` and `asset_classes` mapping.
