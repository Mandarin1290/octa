from datetime import date

from octa_assets.commodities.seasonality import SeasonalityModel
from octa_assets.commodities.specs import CommodityRegistry, CommoditySpec


def test_delivery_window_blocks_holding():
    today = date.today()
    spec = CommoditySpec(
        symbol="WHT",
        delivery_months=[today.month],
        storage_cost_per_month=1.0,
        delivery_window_days=10,
    )
    sentinel = type(
        "S",
        (),
        {"last": None, "set_gate": lambda self, l, r: setattr(self, "last", (l, r))},
    )()
    reg = CommodityRegistry(audit_fn=lambda e, p: None, sentinel_api=sentinel)
    reg.register(spec)
    ok = reg.enforce_delivery_guard("WHT", today)
    assert ok is False
    assert sentinel.last is not None and sentinel.last[0] == 3


def test_seasonality_applied():
    # set seasonality factors where month 1 -> 1.1
    factors = {1: 1.1}
    sm = SeasonalityModel(factors)
    v = sm.apply(100.0, date(2025, 1, 15))
    assert abs(v - 110.0) < 1e-9
