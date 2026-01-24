from octa_fund.performance_fee import PerformanceFeeEngine
from octa_fund.share_classes import ShareClass
from octa_ledger.core import AuditChain


def test_hwm_respected_and_fee_computed():
    ledger = AuditChain()
    sc = ShareClass(
        class_id="PC1",
        currency="USD",
        launch_date="2022-01-01T00:00:00Z",
        shares_outstanding=100.0,
        cash_balance=1000.0,
        performance_fee=0.2,
        high_water_mark=1000.0,
    )
    # increase assets to raise total to 1500
    sc.allocate_asset("X", 500.0)

    engine = PerformanceFeeEngine(
        audit_fn=lambda e, p: ledger.append({"event": e, **p})
    )
    fee = engine.crystallize_fee(sc)

    # fee base = current_total - hwm = 1500 - 1000 = 500; fee = 500 * 0.2 = 100
    assert abs(fee - 100.0) < 1e-8
    # HWM updated to post-fee total = 1500 - 100 = 1400
    assert abs(sc.high_water_mark - 1400.0) < 1e-8


def test_no_fee_below_hwm_and_no_double_charge():
    ledger = AuditChain()
    sc = ShareClass(
        class_id="PC2",
        currency="USD",
        launch_date="2022-01-01T00:00:00Z",
        shares_outstanding=100.0,
        cash_balance=1000.0,
        performance_fee=0.2,
        high_water_mark=1200.0,
    )
    # current total = 1000 (below HWM)
    engine = PerformanceFeeEngine(
        audit_fn=lambda e, p: ledger.append({"event": e, **p})
    )
    fee = engine.crystallize_fee(sc)
    assert fee == 0.0

    # raise assets to 1300 and crystallize
    sc.allocate_asset("Y", 300.0)
    fee2 = engine.crystallize_fee(sc)
    assert fee2 > 0.0

    # calling again immediately should not double-charge
    fee3 = engine.crystallize_fee(sc)
    assert fee3 == 0.0
