from datetime import date, timedelta

from octa_assets.rates.curve import CurveBuckets
from octa_assets.rates.duration import BondSpec
from octa_assets.rates.dv01 import aggregate_dv01


def test_dv01_aggregates_correctly():
    today = date.today()
    b1 = BondSpec(
        identifier="B1",
        maturity=today + timedelta(days=365 * 5),
        duration=None,
        modified_duration=5.0,
        convexity=None,
    )
    b2 = BondSpec(
        identifier="B2",
        maturity=today + timedelta(days=365 * 10),
        duration=None,
        modified_duration=7.0,
        convexity=None,
    )
    pos = [(b1, 100000, 1.0), (b2, 50000, 1.0)]
    total = aggregate_dv01(pos)
    # DV01 = md * notional * price * 0.0001
    expected = 5.0 * 100000 * 0.0001 + 7.0 * 50000 * 0.0001
    assert abs(total - expected) < 1e-6


def test_stress_shifts_applied():
    rates = {0.5: 0.005, 2.0: 0.01, 5.0: 0.02, 10.0: 0.03}
    cb = CurveBuckets(rates)
    parallel = cb.apply_stress({"parallel": 0.001})
    assert abs(parallel[2.0] - (0.01 + 0.001)) < 1e-9

    steep = cb.apply_stress({"steepen": (0.0005, -0.0005)})
    # short tenor shifted up
    assert abs(steep[0.5] - (0.005 + 0.0005)) < 1e-9
    # long tenor shifted down
    assert abs(steep[10.0] - (0.03 - 0.0005)) < 1e-9
