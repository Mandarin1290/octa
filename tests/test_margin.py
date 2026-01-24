from octa_core.margin import InstrumentSpec, MarginConfig, PortfolioMarginCalculator
from octa_sentinel.margin_gates import MarginGates


def test_futures_margin_applied():
    specs = {
        "FUT_ES": InstrumentSpec(
            instrument_id="FUT_ES",
            instrument_type="future",
            contract_multiplier=50.0,
            margin_initial_rate=0.05,
            margin_maintenance_rate=0.03,
        )
    }
    calc = PortfolioMarginCalculator(equity=100000.0, specs=specs)
    positions = [
        {"instrument_id": "FUT_ES", "quantity": 1, "price": 4200.0, "side": "long"}
    ]
    res = calc.compute(positions)
    # initial margin should be notional*init_rate = 1*4200*50*0.05
    expected = 1 * 4200.0 * 50.0 * 0.05
    assert abs(res["initial_margin"] - expected) < 1e-6


def test_short_borrow_increases_cost():
    specs = {
        "AAPL": InstrumentSpec(
            instrument_id="AAPL",
            instrument_type="equity",
            margin_initial_rate=0.5,
            margin_maintenance_rate=0.3,
        )
    }
    cfg = MarginConfig(borrow_rate_annual=0.1)
    calc = PortfolioMarginCalculator(equity=100000.0, specs=specs, config=cfg)
    positions = [
        {"instrument_id": "AAPL", "quantity": 100, "price": 150.0, "side": "short"}
    ]
    res = calc.compute(positions)
    notional = 100 * 150.0
    assert res["borrow_cost_annual"] == notional * 0.1


def test_breach_freezes_orders():
    specs = {
        "AAPL": InstrumentSpec(
            instrument_id="AAPL", instrument_type="equity", margin_initial_rate=0.5
        )
    }
    calc = PortfolioMarginCalculator(equity=1000.0, specs=specs)
    positions = [
        {"instrument_id": "AAPL", "quantity": 100, "price": 150.0, "side": "long"}
    ]
    res = calc.compute(positions)
    gates = MarginGates(warn_threshold=0.6, freeze_threshold=0.9)

    class MockSentinel:
        def __init__(self):
            self.last = None

        def set_gate(self, level, reason):
            self.last = (level, reason)

    class MockAllocator:
        def __init__(self):
            self.scaled = None

        def scale_risk(self, factor):
            self.scaled = factor

    sentinel = MockSentinel()
    allocator = MockAllocator()
    action = gates.evaluate_and_act(
        res, sentinel_api=sentinel, allocator_api=allocator, audit_fn=None
    )
    assert action["gate_level"] >= 2
    if action["gate_level"] >= 3:
        assert allocator.scaled == 0.0
