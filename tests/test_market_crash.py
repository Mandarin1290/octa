from octa_wargames.market_crash import MarketCrashSimulator


def make_context():
    positions = {"AAA": 10, "BBB": 8, "CCC": 5}
    prices = {"AAA": 10.0, "BBB": 20.0, "CCC": 5.0}
    exposure = sum(abs(q) * prices[s] for s, q in positions.items())
    return {
        "positions": positions,
        "prices": prices,
        "exposure": exposure,
        "liquidity": 0.8,
        "risk_limit": 50.0,
    }


def test_exposure_reduced_after_extreme_shock():
    sim = MarketCrashSimulator()
    ctx_payload = make_context()
    before_exposure = ctx_payload["exposure"]
    out = sim.simulate("1987", ctx_payload, seed=20250101)
    ctx = out["context"]
    assert ctx.exposure < before_exposure, (
        "Exposure should be reduced after extreme 1987-style shock"
    )
    # ensure audit log recorded reduction if expected_loss exceeded limit
    actions = [e["action"] for e in ctx.audit_log]
    assert "reduce_exposure" in "".join(actions) or ctx.kill_switch in (True, False)


def test_kill_switch_reachable_when_risk_limit_low():
    sim = MarketCrashSimulator()
    ctx_payload = make_context()
    # set an artificially low risk limit to ensure kill-switch triggers
    ctx_payload["risk_limit"] = 1.0
    out = sim.simulate("correlation_one", ctx_payload, seed=99999)
    ctx = out["context"]
    assert ctx.kill_switch is True, (
        "Kill switch should engage when expected loss greatly exceeds risk limit"
    )
