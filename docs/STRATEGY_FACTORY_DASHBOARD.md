# Strategy Factory Dashboard

Purpose
-------
Deterministic oversight view derived only from the canonical `StrategyRegistry` and the append-only `AuditChain` ledger.

Inputs
------
- `StrategyRegistry.list()` — canonical strategy metadata (including `lifecycle_state` and `risk_budget`).
- `AuditChain` — chronological blocks whose `payload` dictionaries include an `event` key and strategy-scoped fields (e.g. `strategy_id`, `usage`, `params`, `new_aum`).

Fields produced
---------------
- `lifecycle_state`: current lifecycle from registry.
- `risk_budget_utilization`: per-strategy utilizations (vol, dd, exposure, max) computed from `risk.register` and `risk.usage` audit events.
- `capacity_utilization`: computed from `capacity.register` params and most recent `capacity.allocate` `new_aum` value.
- `promotion_blockers`: deterministic list of failed gate events (paper/shadow) and risk warnings referencing the strategy.
- `suspension_reasons`: recorded suspension/kill events referencing the strategy.

Determinism guarantees
----------------------
Report output is deterministic provided the ledger is unmodified: strategies are ordered by `strategy_id`, and blocker/reason lists are deduplicated and sorted.

Usage
-----
Use `octa_reports.strategy_factory.StrategyFactoryReport(registry, ledger).build()` to obtain the dashboard payload.
