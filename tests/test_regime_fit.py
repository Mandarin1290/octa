from octa_strategy.regime_fit import RegimeFitEngine


def test_regime_mismatch_detected():
    # market indicator with two regimes: first half low, second half high
    market = [0.1] * 100 + [1.0] * 50
    engine = RegimeFitEngine()
    tags = engine.tag_regimes(market)
    assert len(tags) == len(market)

    # strategy performs well in LOW, poorly in HIGH
    strat = [0.01] * 100 + [-0.02] * 50
    perf = engine.performance_by_regime(strat, tags)

    # current market is last value (1.0) => HIGH
    score, cur, conf = engine.compatibility_score(1.0, market, perf)
    assert cur == "HIGH"
    assert score < 0.5
    alerts = engine.deterioration_alert(perf, strat, alpha=0.1, threshold_std=0.1)
    assert any("REGIME_HIGH" in a for a in alerts)


def test_compatible_strategy_favored():
    market = [0.0] * 80 + [0.8] * 40
    engine = RegimeFitEngine()
    tags = engine.tag_regimes(market)

    # strategy performs slightly better in HIGH
    strat = [0.005] * 80 + [0.02] * 40
    perf = engine.performance_by_regime(strat, tags)

    score_high, cur, conf = engine.compatibility_score(0.8, market, perf)
    assert cur == "HIGH"
    assert score_high > 0.5
