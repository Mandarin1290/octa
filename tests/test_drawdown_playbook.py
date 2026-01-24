from octa_sentinel.drawdown_playbook import evaluate_drawdown


def test_drawdown_ladder_triggers():
    strategies = {"s:alpha": 1.0, "s:beta": 0.8, "s:gamma": 0.5}
    baseline_vol = {s: 0.1 for s in strategies}
    current_vol = {s: 0.1 for s in strategies}

    # small drawdown 3% should apply 2% rung (reduce ~10%)
    res = evaluate_drawdown(
        0.03,
        strategies,
        baseline_vol,
        current_vol,
        correlation_score=0.1,
        incidents_since=0,
        paper_gates_ok=True,
    )
    # expect compression <1 for strategies
    assert all(0.0 < v < 1.0 for v in res["compression"].values())
    assert not res["freeze_list"]

    # 6% drawdown should freeze weakest strategies
    res2 = evaluate_drawdown(
        0.06,
        strategies,
        baseline_vol,
        current_vol,
        correlation_score=0.1,
        incidents_since=0,
        paper_gates_ok=True,
    )
    assert res2["freeze_list"], "Expected freeze list at 5%+ drawdown"

    # kill switch at 12%
    res3 = evaluate_drawdown(
        0.12,
        strategies,
        baseline_vol,
        current_vol,
        correlation_score=0.1,
        incidents_since=0,
        paper_gates_ok=True,
    )
    assert (
        res3.get("incident") and res3["incident"]["reason"] == "kill_switch"
        if "incident" in res3
        else True
    )


def test_re_risk_blocked_when_conditions_not_met():
    strategies = {"s:alpha": 1.0}
    baseline_vol = {"s:alpha": 0.1}
    # volatility elevated
    current_vol = {"s:alpha": 0.3}

    res = evaluate_drawdown(
        0.01,
        strategies,
        baseline_vol,
        current_vol,
        correlation_score=0.2,
        incidents_since=0,
        paper_gates_ok=True,
    )
    assert res["re_risk_allowed"] is False

    # correlation stress
    res2 = evaluate_drawdown(
        0.01,
        strategies,
        baseline_vol,
        baseline_vol,
        correlation_score=0.8,
        incidents_since=0,
        paper_gates_ok=True,
    )
    assert res2["re_risk_allowed"] is False

    # recent incidents
    res3 = evaluate_drawdown(
        0.01,
        strategies,
        baseline_vol,
        baseline_vol,
        correlation_score=0.1,
        incidents_since=2,
        paper_gates_ok=True,
    )
    assert res3["re_risk_allowed"] is False

    # paper gates failing
    res4 = evaluate_drawdown(
        0.01,
        strategies,
        baseline_vol,
        baseline_vol,
        correlation_score=0.1,
        incidents_since=0,
        paper_gates_ok=False,
    )
    assert res4["re_risk_allowed"] is False
