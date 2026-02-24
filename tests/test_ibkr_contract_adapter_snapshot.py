from octa.execution.runner import _extract_nav
from octa_vertex.broker.ibkr_contract import IBKRContractAdapter


def test_account_snapshot_contains_positive_nav_and_currency() -> None:
    snap = IBKRContractAdapter().account_snapshot()

    accepted_nav_keys = {
        "net_liquidation",
        "netLiquidation",
        "nav",
        "equity",
        "account_equity",
        "total_equity",
        "totalEquity",
    }
    present_nav_keys = accepted_nav_keys.intersection(snap.keys())
    assert present_nav_keys

    nav_key = next(iter(present_nav_keys))
    nav_value = snap[nav_key]
    assert isinstance(nav_value, float)
    assert nav_value > 0.0

    currency = snap.get("currency")
    assert isinstance(currency, str)
    assert currency.strip()


def test_runner_extract_nav_accepts_contract_snapshot() -> None:
    snap = IBKRContractAdapter().account_snapshot()
    nav, nav_key = _extract_nav(snap)
    assert nav_key == "net_liquidation"
    assert isinstance(nav, float)
    assert nav > 0.0
