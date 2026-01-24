from octa_alpha.pre_risk import run_pre_risk


def test_excessive_tail_risk_blocked():
    # many severe negative returns
    returns = [-0.1] * 100
    res = run_pre_risk(returns, max_tail_prob=0.05, tail_threshold=-0.05)
    assert not res["passed"]
    assert "excessive_tail_risk" in res["reasons"]


def test_correlation_breach_rejected():
    # signal and existing returns identical -> correlation 1.0
    returns = [0.01] * 50
    res = run_pre_risk(
        returns, signal_returns=returns, existing_returns=returns, max_correlation=0.5
    )
    assert not res["passed"]
    assert "correlation_breach" in res["reasons"]
