# Strategy Registry

Overview
--------
Canonical registry for all strategies in OCTA. Each strategy must be registered before execution. Entries are versioned and immutable except for lifecycle state updates.

Fields
------
- `strategy_id` — unique identifier
- `owner` — human or module responsible
- `asset_classes` — list of asset classes
- `risk_budget` — numeric risk budget
- `holding_period_days` — intended holding period in days
- `expected_turnover_per_month` — numeric
- `lifecycle_state` — current lifecycle state
- `created_at` — ISO timestamp

Behavior
--------
- Duplicate `strategy_id` registrations are rejected.
- Only `lifecycle_state` may be changed through `update_lifecycle()`; all other fields are immutable.
- Every register or lifecycle update emits an audit event with a versioned history.

Use
---
Use `octa_strategy.registry.StrategyRegistry` in governance and pre-execution checks to assert registration and obtain canonical metadata.
