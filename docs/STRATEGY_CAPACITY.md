# Strategy Capacity Model

Overview
--------
Per-strategy capacity limits estimate how much capital a strategy can absorb before expected alpha decays due to market impact and turnover.

Model
-----
Capacity estimate (simple heuristic):

capacity = ADV * adv_fraction * (1 / impact) * (1 / turnover) * base_scaler

Where:
- `ADV` — average daily volume for the strategy's traded instruments
- `adv_fraction` — fraction of ADV considered tradable by the strategy (default 1%)
- `impact` — per-unit market impact estimate (higher reduces capacity)
- `turnover` — expected turnover (higher turnover reduces capacity)

API
---
- `CapacityEngine.register_strategy(...)`
- `estimate_capacity(strategy_id)`
- `capacity_utilization(strategy_id)`
- `allocate(strategy_id, amount)` — attempts to allocate; blocked if exceeding capacity.

Integration
-----------
- Provide `allocator_api` implementing `allocate(strategy_id, amount)` to enact allocations.
- On blocked allocation `sentinel_api.set_gate(2, reason)` is called and an audit event emitted.
