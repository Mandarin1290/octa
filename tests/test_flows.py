from datetime import datetime, timedelta, timezone

import pytest

from octa_capital.flows import CapitalFlows, InsufficientLiquidity, TradingWindowActive


def test_no_movement_during_trading_window():
    cf = CapitalFlows(settlement_delay_days=1, initial_liquid_assets=1000.0)
    cf.set_trading_window(True)
    with pytest.raises(TradingWindowActive):
        cf.redeem("inv1", shares=1.0, nav_per_share=100.0)


def test_subscription_settlement_delay_and_liquidity():
    cf = CapitalFlows(settlement_delay_days=2, initial_liquid_assets=0.0)
    now = datetime.now(timezone.utc)
    cf.subscribe("inv1", amount=500.0, now=now)
    # before settlement liquidity unchanged
    assert cf.get_liquidity() == 0.0
    # after settlement
    cf.process_settlements(now=now + timedelta(days=3))
    assert cf.get_balance("inv1") == pytest.approx(500.0)
    assert cf.get_liquidity() == pytest.approx(500.0)


def test_redemption_liquidity_respected():
    cf = CapitalFlows(settlement_delay_days=1, initial_liquid_assets=100.0)
    # investor balance set up
    cf.investor_balances["inv2"] = 200.0
    # request redemption for 150 -> allowed because <= liquid_assets? (100) -> should raise
    with pytest.raises(InsufficientLiquidity):
        cf.redeem("inv2", shares=1.5, nav_per_share=100.0)

    # if liquid assets are increased, redemption allowed
    cf.liquid_assets = 300.0
    cf.redeem("inv2", shares=1.5, nav_per_share=100.0, now=datetime.now(timezone.utc))
    # not yet settled
    assert cf.get_balance("inv2") == pytest.approx(200.0)
    cf.process_settlements(now=datetime.now(timezone.utc) + timedelta(days=2))
    # balance reduced by 150
    assert cf.get_balance("inv2") == pytest.approx(50.0)
    # liquid assets reduced accordingly
    assert cf.get_liquidity() == pytest.approx(150.0)
