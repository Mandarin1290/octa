# Capacity Model

This document describes the conservative capacity model implemented in `octa_core.capacity`.

Formulas & assumptions
----------------------
- Input per asset: `adv` (shares/contracts per day), `price`, `spread_bps`, `vol`, `tick_size`, `contract_multiplier`.
- Daily ADV notional = `adv * price * contract_multiplier`.
- Capacity is min( max_pct_adv_per_day * daily_ADV_notional,
  max_impact_bps/10000 * daily_ADV_notional ). This is intentionally conservative.
- Stress multiplier reduces allowed size by dividing capacity by `stress_multiplier`.
- If ADV is missing or zero the asset is considered ineligible and must be blocked.

Limitations
-----------
- Simplified linear impact proxy is used rather than complex market impact models. This is deliberate for transparency.
- Model assumes availability of reliable ADV. If ADV is not present, the asset is ineligible by policy.

Integration
-----------
- `CapacityEngine.compute_max_notional` is intended to be called by the allocator before accepting target positions.
- `compute_slice_limits` and `octa_vertex.slicing.vwap_slices` are used to cap and produce slice schedules.
