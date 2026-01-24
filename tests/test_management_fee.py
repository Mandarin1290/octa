from octa_fund.management_fee import ManagementFeeEngine
from octa_fund.share_classes import ShareClassSeries
from octa_ledger.core import AuditChain


def test_management_fee_accrual_reduces_nav_and_audited():
    ledger = AuditChain()
    series = ShareClassSeries(
        fund_id="FEEF", audit_fn=lambda e, p: ledger.append({"event": e, **p})
    )
    series.create_class(
        class_id="C1",
        currency="USD",
        launch_date="2022-01-01T00:00:00Z",
        initial_shares=100.0,
        initial_cash=1000.0,
        management_fee_annual=0.10,
    )
    c1 = series.get_class("C1")

    engine = ManagementFeeEngine(audit_fn=lambda e, p: ledger.append({"event": e, **p}))

    before_total = c1.total_value()
    # expected fee for one day
    expected_fee = before_total * 0.10 * (1.0 / 365.0)

    fees = engine.accrue_daily(series)
    assert "C1" in fees
    assert abs(fees["C1"] - expected_fee) < 1e-8

    after_total = c1.total_value()
    # NAV reduced by fee amount
    assert abs(before_total - after_total - fees["C1"]) < 1e-8

    # audit events exist: per-class mgmt_fee and daily summary
    events = [b for b in ledger._chain if isinstance(b.payload, dict)]
    names = [b.payload.get("event") for b in events if isinstance(b.payload, dict)]
    assert "shareclass.mgmt_fee" in names
    assert "management_fee.daily_accrual" in names
