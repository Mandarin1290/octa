import pytest

from octa_capital.accounts import CapitalAccount, CapitalError


def test_balance_consistency_and_reservations():
    acc = CapitalAccount("main")
    acc.deposit(100000, actor="funding")
    acc.reserve("s1", 20000, actor="ops")
    acc.reserve("s2", 50000, actor="ops")

    balances = acc.get_balances()
    assert balances["total"] == 100000
    assert balances["deployable"] == 30000
    assert balances["reserved"]["s1"] == 20000
    assert balances["reserved"]["s2"] == 50000

    # consume some reserved from s1
    acc.consume_reserved("s1", 10000, actor="execution")
    balances2 = acc.get_balances()
    assert balances2["total"] == 90000
    assert balances2["reserved"]["s1"] == 10000
    # deployable recomputed
    assert balances2["deployable"] == 90000 - (10000 + 50000)


def test_isolation_and_limits_enforced():
    acc = CapitalAccount("main")
    acc.deposit(50000, actor="funding")
    acc.reserve("s1", 30000, actor="ops")

    # cannot reserve more than deployable
    with pytest.raises(CapitalError):
        acc.reserve("s2", 25000, actor="ops")

    # cannot consume more than reserved
    with pytest.raises(CapitalError):
        acc.consume_reserved("s1", 40000, actor="execution")

    # transfer between subaccounts enforces from-reserved
    acc.reserve("s2", 10000, actor="ops")
    with pytest.raises(CapitalError):
        acc.transfer_between_subaccounts("s2", "s3", 20000, actor="ops")
