from decimal import Decimal

from octa_fund.investor_accounts import InvestorAccount
from octa_fund.subscriptions import RedemptionManager, SubscriptionManager


def test_nav_pricing_subscription():
    acct = InvestorAccount("inv-s1", "Subber")
    sm = SubscriptionManager()
    # request subscription of 100 at NAV
    sm.request_subscription(acct, "class-A", Decimal("100"))
    processed = sm.process_subscriptions(Decimal("10"))
    assert len(processed) == 1
    shares = acct.shares.get("class-A")
    assert shares == Decimal("10").quantize(Decimal("0.00000001"))
    assert acct.reconcile()


def test_liquidity_respected_for_redemption():
    acct = InvestorAccount("inv-r1", "Redeemer")
    # seed and buy shares
    acct.deposit(Decimal("1000"))
    acct.buy_shares("class-A", Decimal("50"), Decimal("10"))  # cost 500
    # request redemption of all 50 shares
    rm = RedemptionManager()
    rm.request_redemption(acct, "class-A", Decimal("50"))
    # available liquid only 100 -> cannot process
    processed = rm.process_redemptions(Decimal("10"), Decimal("100"))
    assert processed == []
    assert len(rm.queue) == 1
    # now liquidity increases sufficiently
    processed2 = rm.process_redemptions(Decimal("10"), Decimal("600"))
    assert len(processed2) == 1
    assert acct.shares.get("class-A", Decimal("0")) == Decimal("0")
    assert acct.reconcile()
