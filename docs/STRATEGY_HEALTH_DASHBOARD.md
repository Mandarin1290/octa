# Strategy Health Dashboard (Aging & Health)

Purpose
-------
Produce a deterministic, explainable dashboard extending the Strategy Factory with aging tiers, health scores, decay warnings, regime fit summaries, auto-derisk status and sunset candidates.

Usage
-----
Use `StrategyHealthDashboard(registry, ledger, health_scorer=..., alpha_detector=..., regime_engine=..., stability_analyzer=..., auto_derisk=..., sunset_engine=...).build(returns_by_strategy=..., market_indicator=...)`.

Determinism
----------
Strategies are ordered by `strategy_id`. Derived fields use canonical computations and stable thresholds.
