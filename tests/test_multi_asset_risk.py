from octa_core.multi_asset_risk import MultiAssetRiskEngine


def test_worst_case_dominates():
    exposures = {
        "equities": 100.0,
        "futures": 10.0,
        "fx": 5.0,
        "rates": 20.0,
        "vol": 2.0,
    }
    engine = MultiAssetRiskEngine()
    # compute per-scenario margins directly to assert worst-case > base
    worst = engine.worst_case_margin(exposures)
    base_margin = engine.margin_for_scenario(
        exposures, engine.DEFAULT_STRESS_SCENARIOS["base"]
    )
    assert worst >= base_margin
    # one of the stress scenarios should be strictly larger than base
    assert any(
        engine.margin_for_scenario(exposures, s) > base_margin
        for s in engine.DEFAULT_STRESS_SCENARIOS.values()
    )


def test_breach_freezes_trading():
    exposures = {
        "equities": 1000.0,
        "futures": 0.0,
        "fx": 0.0,
        "rates": 0.0,
        "vol": 0.0,
    }

    class SentinelMock:
        def __init__(self):
            self.last = None

        def set_gate(self, level, reason):
            self.last = (level, reason)

    sentinel = SentinelMock()
    engine = MultiAssetRiskEngine(sentinel_api=sentinel)
    # give tiny capital so worst-case margin > capital
    report = engine.assess_and_enforce(exposures, capital=10.0, leverage_limit=100.0)
    assert report["breach"] is True
    assert sentinel.last is not None and sentinel.last[0] == 3
