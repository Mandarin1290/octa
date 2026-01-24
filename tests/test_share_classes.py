from octa_fund.share_classes import ShareClassSeries
from octa_ledger.core import AuditChain


def test_class_isolation_and_nav_separation():
    ledger = AuditChain()
    series = ShareClassSeries(
        fund_id="F1", audit_fn=lambda e, p: ledger.append({"event": e, **p})
    )
    series.create_class(
        class_id="C1",
        currency="USD",
        launch_date="2022-01-01T00:00:00Z",
        initial_shares=100.0,
        initial_cash=1000.0,
    )
    series.create_class(
        class_id="C2",
        currency="USD",
        launch_date="2023-01-01T00:00:00Z",
        initial_shares=50.0,
        initial_cash=500.0,
    )

    c1 = series.get_class("C1")
    c2 = series.get_class("C2")

    # allocate assets separately
    c1.allocate_asset("A", 900.0)  # c1 total = 1900
    c2.allocate_asset("B", 0.0)  # c2 total = 500

    nav1 = c1.compute_nav()
    nav2 = c2.compute_nav()

    assert nav1["nav_per_share"] == 1900.0 / 100.0
    assert nav2["nav_per_share"] == 500.0 / 50.0

    # deposit to C1 does not affect C2
    c1.deposit(100.0)
    assert c1.cash_balance == 1100.0
    assert c2.cash_balance == 500.0

    # redeem from C2 reduces only C2
    c2.redeem(10.0)
    assert c2.shares_outstanding == 40.0
