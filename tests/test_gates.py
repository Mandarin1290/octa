import pytest

from octa_capital.gates import CapitalGates


def test_gate_enforcement_blocks_when_closed():
    g = CapitalGates()
    g.set_redemption_gate(False)
    res = g.evaluate_redemption(
        requested_value=100.0,
        investor_balance=200.0,
        liquid_assets=500.0,
        portfolio_value=1000.0,
    )
    assert res["blocked"] is True
    assert res["allowed"] == 0.0
    assert res["reason"] == "gate_closed"


def test_side_pocket_allocation_applies():
    g = CapitalGates()
    g.enable_side_pocket(0.5)
    # with ample liquidity and balance, request 100 -> allowed 50 net, 50 side-pocket
    res = g.evaluate_redemption(
        requested_value=100.0,
        investor_balance=200.0,
        liquid_assets=500.0,
        portfolio_value=1000.0,
    )
    assert pytest.approx(res["allowed"]) == 50.0
    assert pytest.approx(res["side_pocket"]) == 50.0


def test_stress_limits_reduce_allowed():
    g = CapitalGates(base_limit_percent=0.2, stress_sensitivity=1.0)
    # baseline allowed by stress = 0.2 * 1000 = 200
    res0 = g.evaluate_redemption(
        requested_value=500.0,
        investor_balance=1000.0,
        liquid_assets=500.0,
        portfolio_value=1000.0,
    )
    assert res0["allowed"] <= 200.0

    # at high stress, cap reduces to near zero
    g.set_stress_metric(1.0)
    res1 = g.evaluate_redemption(
        requested_value=500.0,
        investor_balance=1000.0,
        liquid_assets=500.0,
        portfolio_value=1000.0,
    )
    assert res1["allowed"] <= res0["allowed"]
    assert res1["allowed"] >= 0.0
