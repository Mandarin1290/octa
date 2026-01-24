from decimal import Decimal

import pytest

from octa_fund.gates import LockupManager, RedemptionGateManager
from octa_fund.investor_accounts import InvestorAccount
from octa_fund.side_pockets import SidePocketManager


def test_gate_enforced():
    gm = RedemptionGateManager(Decimal("0.05"))
    aum = Decimal("1000")
    allowed = gm.allowed_amount(aum)
    assert allowed == Decimal("50").quantize(Decimal("0.00000001"))
    assert gm.allow_redemption(Decimal("30"), aum)
    assert not gm.allow_redemption(Decimal("100"), aum)


def test_lockup_prevents_redemption():
    acct = InvestorAccount("inv-lock", "Locky")
    acct.deposit(Decimal("1000"))
    acct.buy_shares("class-X", Decimal("10"), Decimal("10"))
    lm = LockupManager()
    # lock 8 shares for 10 days
    lm.add_lock(acct.investor_id, "class-X", Decimal("8"), lock_period_days=10)
    # attempting to redeem 5 should be blocked (available = 2)
    assert not lm.is_redeemable(acct, "class-X", Decimal("5"))
    # redeeming 2 should be allowed
    assert lm.is_redeemable(acct, "class-X", Decimal("2"))


def test_side_pockets_isolation_and_redemption():
    acct = InvestorAccount("inv-pocket", "Pocker")
    acct.deposit(Decimal("1000"))
    acct.buy_shares("class-A", Decimal("100"), Decimal("10"))
    sp = SidePocketManager()
    sp.move_to_pocket(acct, "class-A", "illiquid-1", Decimal("40"))
    # available main shares = 60
    assert sp.available_main_shares(acct, "class-A") == Decimal("60")
    # cannot redeem more than available main shares via main redemption
    with pytest.raises(ValueError):
        sp.redeem_from_main(acct, "class-A", Decimal("70"), Decimal("10"))
    # redeem 50 from main should succeed
    sp.redeem_from_main(acct, "class-A", Decimal("50"), Decimal("10"))
    assert acct.shares.get("class-A") == Decimal("50")  # 100 - 50 sold
    # pocketed shares still 40
    assert sp.pocketed_shares(acct, "class-A") == Decimal("40")
    # cannot redeem pocket without approval
    with pytest.raises(ValueError):
        sp.redeem_from_pocket(
            acct, "class-A", "illiquid-1", Decimal("10"), Decimal("10"), approved=False
        )
    # with approval, pocket redemption allowed
    sp.redeem_from_pocket(
        acct, "class-A", "illiquid-1", Decimal("10"), Decimal("10"), approved=True
    )
    assert sp.pocketed_shares(acct, "class-A") == Decimal("30")
