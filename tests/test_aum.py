from octa_capital.aum_state import AUMState
from octa_ledger.core import AuditChain


def test_aum_updates_and_reconciliation():
    ledger = AuditChain()
    aum = AUMState(
        audit_fn=lambda e, p: ledger.append({"event": e, **p}),
        initial_internal=1000.0,
        initial_external=500.0,
    )

    # initial snapshot: computed 1500, reported 1500
    snap0 = aum.snapshot(portfolio_value=1500.0)
    assert snap0.computed_total == 1500.0
    assert snap0.reported_total == 1500.0
    assert snap0.reconciled is True

    # apply pnl increases internal -> computed becomes 1600
    aum.apply_pnl(100.0, reason="test pnl")
    snap1 = aum.snapshot(portfolio_value=1600.0)
    assert snap1.internal_capital == 1100.0
    assert snap1.computed_total == 1600.0
    assert snap1.reported_total == 1600.0
    assert snap1.reconciled is True

    # external inflow that doesn't match portfolio -> reconciliation false
    aum.inflow(100.0, source="external", reason="investor capital")
    snap2 = aum.snapshot(portfolio_value=1700.0)  # computed 1700
    assert snap2.computed_total == 1700.0
    assert snap2.reconciled is True

    # outflow causing mismatch
    aum.outflow(50.0, source="internal", reason="redemption")
    snap3 = aum.snapshot(portfolio_value=1700.0)  # computed 1650 vs reported 1700
    assert snap3.computed_total == 1650.0
    assert snap3.reported_total == 1700.0
    assert snap3.reconciled is False

    # audit events emitted (AuditChain stores blocks in _chain)
    assert len(ledger._chain) >= 5


def test_downstream_subscriber_sees_updates():
    aum = AUMState(initial_internal=200.0, initial_external=300.0)
    seen = []

    def consumer(snap):
        seen.append(snap)

    aum.subscribe(consumer)
    aum.inflow(100.0, source="external", reason="test")
    aum.snapshot(portfolio_value=600.0)

    assert len(seen) == 1
    assert seen[0].reported_total == 600.0
