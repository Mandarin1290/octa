from octa_core.cross_asset_corr import CrossAssetCorrelation


def correlated_series(base: list, factor: float) -> list:
    return [b * factor for b in base]


def test_stress_regime_increases_correlations():
    # base random-ish series
    base = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01]
    # risk-on: assets less correlated
    returns_on = {
        "A": base,
        "B": [v * 0.3 for v in base],
        "C": [v * -0.2 + (0.001 if i % 2 == 0 else -0.001) for i, v in enumerate(base)],
    }
    # risk-off: assets move together (high correlation)
    returns_off = {
        "A": base,
        "B": correlated_series(base, 0.9),
        "C": correlated_series(base, 0.8),
    }

    engine = CrossAssetCorrelation()
    r_on = engine.assess_and_escalate(returns_on, regime="risk-on")
    r_off = engine.assess_and_escalate(returns_off, regime="risk-off")

    assert r_off["mean_corr"] > r_on["mean_corr"]


def test_escalation_triggers_compression_and_sentinel():
    base = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01]
    returns = {
        "A": base,
        "B": correlated_series(base, 0.95),
        "C": correlated_series(base, 0.9),
    }

    class SentinelMock:
        def __init__(self):
            self.last = None

        def set_gate(self, level, reason):
            self.last = (level, reason)

    sentinel = SentinelMock()
    engine = CrossAssetCorrelation(sentinel_api=sentinel, spike_threshold=0.05)
    report = engine.assess_and_escalate(returns, regime="risk-off")
    assert report["escalated"] is True
    assert 0.1 <= report["compression"] < 1.0
    assert sentinel.last is not None and sentinel.last[0] == 2
