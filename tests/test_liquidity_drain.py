from octa_wargames.liquidity_drain import LiquidityDrainSimulator


def make_ctx():
    positions = {"AAA": 100, "BBB": 50, "CCC": 20}
    prices = {"AAA": 10.0, "BBB": 5.0, "CCC": 2.0}
    cash = 1000.0
    liquidity = 0.8
    spread = 0.001
    return {
        "positions": positions,
        "prices": prices,
        "cash": cash,
        "liquidity": liquidity,
        "spread": spread,
    }


def test_liquidation_slowed():
    sim = LiquidityDrainSimulator()
    ctx_payload = make_ctx()
    out = sim.simulate("forced_liquidation", ctx_payload, seed=42, steps=3)
    ctx = out["context"]
    # Protector should have limited per-step liquidation; positions should not be zero
    assert any(q > 0 for q in ctx.positions.values())


def test_loss_bounded():
    sim = LiquidityDrainSimulator()
    ctx_payload = make_ctx()
    out = sim.simulate("spread_explosion", ctx_payload, seed=123, steps=5)
    result = out["result"]
    # loss should be bounded reasonably relative to notional (simple check)
    total_notional = sum(
        abs(q) * ctx_payload["prices"][s] for s, q in ctx_payload["positions"].items()
    )
    assert result["loss"] <= total_notional * 0.8
