# Multi-Asset Live-Readiness Gates

Overview
--------
Per-asset readiness gates that must be passed before allowing live trading in that asset class. Gates are independent so failure in one asset class does not block others.

Required gates
--------------
- Futures: `roll_tested` (boolean).
- FX: `funding_ratio` above a threshold (default 0.8).
- Rates: `stress_passed` (boolean).
- Volatility: `exposure` must be ≤ `exposure_cap`.
- Commodities: `delivery_guard` (boolean).

Enforcement
-----------
- Use `octa_sentinel.multi_asset_gates.MultiAssetGates.evaluate_all(status)` where `status` provides gate-specific indicators.
- On gate failure the component calls `sentinel_api.set_gate(3, reason)` for that asset class and writes an audit event.

Integration
-----------
- Call per-asset evaluation during pre-go-live checklist and periodically while in live mode.
