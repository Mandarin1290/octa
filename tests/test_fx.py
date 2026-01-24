from octa_assets.fx.carry import CarryEngine
from octa_assets.fx.exposure import ExposureTracker
from octa_assets.fx.pairs import FXPair


def test_carry_accrual_correct():
    ce = CarryEngine()
    # base_qty 1000, price 1.2, r_base=0.01, r_quote=0.02, 1 day
    accr = ce.daily_accrual(1000, 1.2, 0.01, 0.02, days=1)
    # notional_quote = 1200, diff=0.01 => accr = 1200 * 0.01 / 365
    expected = 1200 * 0.01 / 365.0
    assert abs(accr - expected) < 1e-9


def test_netting_works_and_currency_caps():
    sentinel = type(
        "S",
        (),
        {"last": None, "set_gate": lambda self, l, r: setattr(self, "last", (l, r))},
    )()
    et = ExposureTracker(audit_fn=lambda e, p: None, sentinel_api=sentinel)

    # record two trades across strategies that net in USD
    et.record_trade(
        "A", "s1", base_currency="EUR", quote_currency="USD", base_qty=1000, price=1.1
    )
    et.record_trade(
        "A", "s2", base_currency="EUR", quote_currency="USD", base_qty=-500, price=1.2
    )

    net = et.net_exposure()
    # base exposure EUR = 500; quote exposure USD = -1000*1.1 + 500*1.2 = -1100 + 600 = -500
    assert abs(net.get("EUR", 0) - 500) < 1e-9
    assert abs(net.get("USD", 0) + 500) < 1e-9

    # enforce cap: USD cap 400 should breach
    ok, breaches = et.enforce_caps({"USD": 400.0})
    assert ok is False
    assert "USD" in breaches


def test_pair_pip_value():
    p = FXPair(pair="EURUSD", base="EUR", quote="USD", pip_size=0.0001)
    # pip value per 100000 base units is pip_size * lot
    assert abs(p.pip_value(100000, 1.12) - 0.0001 * 100000) < 1e-9
