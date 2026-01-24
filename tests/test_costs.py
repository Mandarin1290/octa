from octa_vertex.slippage import (
    pre_trade_slippage_estimate,
    sqrt_impact,
    time_of_day_factor,
)


def test_higher_size_higher_impact():
    # larger size relative to adv increases impact
    s1 = sqrt_impact(size=1000, adv=10000, sigma=0.02, impact_coeff=0.2)
    s2 = sqrt_impact(size=2000, adv=10000, sigma=0.02, impact_coeff=0.2)
    assert s2 > s1


def test_illiquid_asset_penalized():
    # smaller ADV -> larger impact
    i1 = sqrt_impact(size=1000, adv=100000, sigma=0.02, impact_coeff=0.2)
    i2 = sqrt_impact(size=1000, adv=1000, sigma=0.02, impact_coeff=0.2)
    assert i2 > i1


def test_time_of_day_factor_stable():
    # deterministic mapping
    f1 = time_of_day_factor("2025-01-01T08:00:00+00:00")
    f2 = time_of_day_factor("2025-01-01T12:00:00+00:00")
    f3 = time_of_day_factor("2025-01-01T23:00:00+00:00")
    assert f1 >= 1.0 and f2 <= 1.0 and f3 >= 1.0


def test_pre_trade_estimate_components():
    res = pre_trade_slippage_estimate(
        size=100,
        price=10.0,
        adv=10000,
        sigma=0.02,
        half_spread=0.01,
        ts="2025-01-01T12:00:00+00:00",
        impact_coeff=0.2,
        fixed_fees=1.0,
    )
    # components present and total equals sum
    assert (
        abs(
            res["total_estimate"]
            - (res["fixed_fees"] + res["spread_cost"] + res["impact_cost"])
        )
        < 1e-9
    )
