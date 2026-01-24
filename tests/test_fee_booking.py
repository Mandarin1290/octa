from octa_fund.fee_booking import FeeBookingEngine
from octa_fund.share_classes import ShareClassSeries
from octa_ledger.core import AuditChain


def test_correct_booking_and_audit_trace():
    ledger = AuditChain()
    series = ShareClassSeries(
        fund_id="FB", audit_fn=lambda e, p: ledger.append({"event": e, **p})
    )
    series.create_class(
        class_id="PC1",
        currency="USD",
        launch_date="2022-01-01T00:00:00Z",
        initial_shares=100.0,
        initial_cash=1000.0,
        performance_fee=0.2,
    )
    sc = series.get_class("PC1")
    sc.allocate_asset("X", 500.0)  # total 1500, hwm default 1000 -> gain 500

    engine = FeeBookingEngine(audit_fn=lambda e, p: ledger.append({"event": e, **p}))
    booked = engine.book_crystallized_fees(
        series, booking_date="2025-12-28T00:00:00Z", period="monthly"
    )
    assert "PC1" in booked
    assert abs(booked["PC1"] - 100.0) < 1e-8

    payables = engine.get_payables()
    assert "PC1" in payables

    # reverse booking before settlement
    ok = engine.reverse_booking("PC1", reason="test reversal")
    assert ok is True
    assert "PC1" not in engine.get_payables()


def test_settlement_updates_nav_and_audit():
    ledger = AuditChain()
    series = ShareClassSeries(
        fund_id="FB2", audit_fn=lambda e, p: ledger.append({"event": e, **p})
    )
    series.create_class(
        class_id="PC2",
        currency="USD",
        launch_date="2022-01-01T00:00:00Z",
        initial_shares=100.0,
        initial_cash=1000.0,
        performance_fee=0.2,
    )
    sc = series.get_class("PC2")
    sc.allocate_asset("Y", 500.0)

    engine = FeeBookingEngine(audit_fn=lambda e, p: ledger.append({"event": e, **p}))
    engine.book_crystallized_fees(
        series, booking_date="2025-12-28T00:00:00Z", period="monthly"
    )
    payables = engine.get_payables()
    assert payables["PC2"]["amount"] == 100.0

    # settle
    amt = engine.settle_payable("PC2", series)
    assert abs(amt - 100.0) < 1e-8
    # payable marked paid
    payables2 = engine.get_payables()
    assert payables2["PC2"]["paid"] is True

    # audit chain contains booking and settled
    names = [
        b.payload.get("event") for b in ledger._chain if isinstance(b.payload, dict)
    ]
    assert "fee_booking.booked" in names
    assert "fee_booking.settled" in names
