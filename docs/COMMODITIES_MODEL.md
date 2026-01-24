# Commodities Model

Overview
--------
Commodity semantics include delivery months, storage costs, seasonality, and delivery guards. Holdings must be rolled before delivery windows and seasonal factors adjust expected supply/demand.

Files
-----
- `octa_assets/commodities/specs.py` — `CommoditySpec` and `CommodityRegistry` with delivery-window enforcement.
- `octa_assets/commodities/seasonality.py` — `SeasonalityModel` applying monthly seasonal factors.

Rules
-----
- Force roll before delivery window; if delivery window is reached, trading for that instrument is frozen via Sentinel.
- Apply seasonal multipliers to expected prices/exposures to account for recurring patterns.
