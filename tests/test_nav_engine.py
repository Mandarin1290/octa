import copy

from octa_accounting.nav_engine import NAVEngine


def test_nav_reconciliation_simple():
    e = NAVEngine()
    e.deposit(100000, actor="funding")
    e.record_trade("A", 100, 100, actor="trader")
    e.update_market_price("A", 110, actor="market")
    e.accrue_fee(50, actor="fees")

    report = e.compute_nav()
    # cash = 100000 - 100*100 - 50 = 89950
    assert round(report["cash"], 8) == 89950
    # market value = 100 * 110 = 11000
    assert round(report["market_value"], 8) == 11000
    # NAV = 89950 + 11000 = 100950
    assert round(report["nav"], 8) == 100950


def test_historical_replay_reproducible():
    e1 = NAVEngine()
    e1.deposit(50000, actor="funding")
    e1.record_trade("B", 200, 50, actor="trader")
    e1.update_market_price("B", 55, actor="market")
    e1.accrue_fee(25, actor="fees")

    report1 = e1.compute_nav()

    # replay on a new engine
    events = copy.deepcopy(e1.history)
    e2 = NAVEngine()
    e2.replay_history(events)
    report2 = e2.compute_nav()

    # reports (excluding timestamps in audit) should match report_hash
    assert report1["report_hash"] == report2["report_hash"]
