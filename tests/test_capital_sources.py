import pytest

from octa_capital.accounts import CapitalError
from octa_capital.capital_sources import CapitalSources


def test_segregation_enforced_and_accounting():
    cs = CapitalSources()
    # deposit internal 100k
    cs.deposit_internal(100000, actor="funding")
    assert cs.strategy_deployable() == 100000

    # deposit investor 200k
    cs.deposit_investor("invA", 200000, actor="funding")
    agg = cs.aggregate_view()
    # investor deposit must not affect strategy deployable
    assert agg["internal"]["total"] == 100000
    assert agg["investors"]["invA"]["total"] == 200000
    assert cs.strategy_deployable() == 100000

    # allocate to strategy must use internal pool
    cs.allocate_to_strategy("strat1", 30000, actor="ops")
    assert cs.strategy_deployable() == 70000

    # attempting to allocate investor funds for strategy decision is disallowed by API (must use allocate_from_investor for execution-only)
    with pytest.raises(CapitalError):
        # try to allocate investor by reserving in internal via bad direct call
        cs.internal.reserve("bad", 1e9, actor="malicious")

    # reserve investor funds for execution is allowed
    cs.allocate_from_investor("invA", "exec1", 50000, actor="ops")
    agg2 = cs.aggregate_view()
    assert agg2["investors"]["invA"]["reserved"]["exec1"] == 50000


def test_accounting_accuracy_with_multiple_investors():
    cs = CapitalSources()
    cs.deposit_internal(50000, actor="funding")
    cs.deposit_investor("inv1", 100000, actor="funding")
    cs.deposit_investor("inv2", 150000, actor="funding")

    agg = cs.aggregate_view()
    assert agg["internal"]["total"] == 50000
    assert agg["investors"]["inv1"]["total"] == 100000
    assert agg["investors"]["inv2"]["total"] == 150000
