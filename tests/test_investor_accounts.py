from decimal import Decimal

import pytest

from octa_fund.investor_accounts import InvestorAccount


def test_isolation_between_investors():
    a1 = InvestorAccount("inv-1", "Alice")
    a2 = InvestorAccount("inv-2", "Bob")
    a1.deposit(Decimal("100"))
    assert a1.balance == Decimal("100")
    assert a2.balance == Decimal("0")
    # a2 remains unaffected
    with pytest.raises(ValueError):
        a2.withdraw(Decimal("1"))


def test_balances_and_shares_reconcile():
    acct = InvestorAccount("inv-3", "Carol")
    acct.deposit(Decimal("100"))
    acct.buy_shares("class-A", Decimal("3"), Decimal("10"))  # cost 30
    acct.sell_shares("class-A", Decimal("1"), Decimal("12"))  # proceeds 12
    # expected balance: 100 - 30 + 12 = 82
    assert acct.balance == Decimal("82").quantize(Decimal("0.00000001"))
    shares = acct.shares
    assert shares.get("class-A") == Decimal("2")
    assert acct.reconcile()


def test_ledger_entries_content():
    acct = InvestorAccount("inv-4", "Dana")
    acct.deposit(Decimal("50"), memo="seed")
    acct.buy_shares("class-B", Decimal("2"), Decimal("5"))
    hist = acct.get_history()
    assert len(hist) == 2
    assert hist[0]["event"] == "cash.deposit"
    assert hist[1]["event"] == "shares.purchase"
    assert hist[1]["details"]["share_class"] == "class-B"
