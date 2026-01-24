# Strategy Risk Budgets

Overview
--------
Per-strategy risk budgets define binding limits on volatility, drawdown and exposure. Budgets are enforced in real time; breaches trigger an escalation ladder (warn → derisk → suspend).

API
---
- `RiskBudget(vol_budget, dd_budget, exposure_budget, ...)` — budget definition.
- `RiskBudgetEngine.register_strategy(strategy_id, budget)` — register canonical budget.
- `record_usage(strategy_id, vol, dd, exposure)` — update live consumption; engine evaluates and escalates automatically.

Escalation
----------
- `util >= warn_threshold` → audit + `sentinel.set_gate(1,...)`
- `util >= derisk_threshold` → call `allocator.derisk(strategy_id, factor)` + `sentinel.set_gate(2,...)`
- `util >= suspend_threshold` → increment suspend counter; after configured repeats call `allocator.suspend(strategy_id)` + `sentinel.set_gate(3,...)`

Integration
-----------
Provide `allocator_api` and `sentinel_api` objects implementing `derisk()`/`suspend()` and `set_gate(level, reason)` respectively.
