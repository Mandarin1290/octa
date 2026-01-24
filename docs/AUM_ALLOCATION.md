# AUM‑Aware Allocation

Overview
--------
`AUMAwareAllocator` sizes strategy positions while explicitly accounting for AUM and per-strategy scalability.

Behavior
--------
- Uses `expected_returns` as relative signals to propose an allocation budget (controlled by `deploy_fraction`).
- Computes per-strategy absolute capacity caps as: `aum_total * base_fraction_of_aum * scale_fn(aum_total)`.
- Caps dominate: allocations never exceed capacity regardless of expected returns.
- If caps bind, remaining budget is redistributed to non-capped strategies proportionally.

Integration
-----------
- Provide `capacity_specs` mapping strategy ids to `StrategyCapacitySpec(base_fraction_of_aum, scale_fn)`.
- Use `aum_state.get_current_total()` to supply `aum_total` when allocating.

Notes
-----
- The `scale_fn` allows strategy-specific scalability curves that reduce permissible fraction as AUM grows.
- This allocator is conservative by design: capacity constraints always apply before return optimization.
