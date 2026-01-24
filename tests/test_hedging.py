from octa_core.hedging import HedgeEngine


def test_hedge_reduces_variance():
    # simple correlated series: hedge ~ 0.9 * asset
    asset = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01]
    hedge = [0.009, -0.018, 0.0135, -0.0045, 0.018, -0.009]
    exposures = {"EQ": 1.0}
    market = {"EQ": asset, "EQ_FUT": hedge}
    engine = HedgeEngine()
    report = engine.assess_and_enforce(exposures, market, regime="normal")
    assert "EQ_FUT" in report
    r = report["EQ_FUT"]["reduction"]
    # expect a meaningful reduction due to high correlation
    assert r > 0.5


def test_ineffective_hedge_flagged():
    # hedge uncorrelated (ineffective)
    asset = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01]
    hedge = [0.002, 0.003, -0.004, 0.001, -0.002, 0.005]
    exposures = {"EQ": 1.0}
    market = {"EQ": asset, "EQ_FUT": hedge}

    class SentinelMock:
        def __init__(self):
            self.last = None

        def set_gate(self, level, reason):
            self.last = (level, reason)

    sentinel = SentinelMock()
    engine = HedgeEngine(sentinel_api=sentinel, effectiveness_threshold=0.05)
    engine.assess_and_enforce(exposures, market, regime="normal")
    # since hedge is poor, sentinel should have been triggered
    assert sentinel.last is not None and sentinel.last[0] == 3
