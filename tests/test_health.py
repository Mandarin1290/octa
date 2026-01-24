from octa_strategy.health import HealthScorer


def test_unhealthy_strategy_flagged():
    scorer = HealthScorer()
    # bad inputs: high decay, bad regime, unstable, sharp crash, risk over 1
    alpha = {"decay_score": 0.9}
    regime = {"compatibility_score": 0.2}
    stability = {"stability_score": 0.85}
    draw = {"profile": {"classification": "SHARP_CRASH"}}
    risk_util = 1.2

    rpt = scorer.score(
        alpha_decay=alpha,
        regime_fit=regime,
        stability=stability,
        drawdown_profile=draw,
        risk_util=risk_util,
    )
    assert rpt.score < 0.4


def test_healthy_strategy_stable():
    scorer = HealthScorer()
    alpha = {"decay_score": 0.0}
    regime = {"compatibility_score": 0.9}
    stability = {"stability_score": 0.1}
    draw = {"profile": {"classification": "QUICK_RECOVERY"}}
    risk_util = 0.2

    rpt = scorer.score(
        alpha_decay=alpha,
        regime_fit=regime,
        stability=stability,
        drawdown_profile=draw,
        risk_util=risk_util,
    )
    assert rpt.score > 0.7
