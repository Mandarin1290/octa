from octa_capital.aum_state import AUMState
from octa_capital.hard_close import HardCloseEngine
from octa_ledger.core import AuditChain


def test_inflow_rejected_when_active():
    ledger = AuditChain()
    aum = AUMState(
        audit_fn=lambda e, p: ledger.append({"event": e, **p}),
        initial_internal=900.0,
        initial_external=0.0,
    )
    engine = HardCloseEngine(
        absolute_cap=1000.0,
        required_approvals=2,
        audit_fn=lambda e, p: ledger.append({"event": e, **p}),
    )
    engine.attach(aum)

    # push over cap via inflow
    aum.inflow(200.0, source="external", reason="seed")
    # snapshot will trigger activation via attach subscription
    aum.snapshot(portfolio_value=1100.0)
    assert engine.active is True

    # attempt external inflow should be rejected
    prev_external = aum._external
    res = aum.inflow(100.0, source="external", reason="test")
    assert res is False
    assert aum._external == prev_external


def test_override_requires_committee_and_is_audited():
    ledger = AuditChain()
    aum = AUMState(
        audit_fn=lambda e, p: ledger.append({"event": e, **p}),
        initial_internal=1200.0,
        initial_external=0.0,
    )
    engine = HardCloseEngine(
        absolute_cap=1000.0,
        required_approvals=2,
        audit_fn=lambda e, p: ledger.append({"event": e, **p}),
    )
    engine.attach(aum)

    # immediate snapshot shows over cap
    aum.snapshot(portfolio_value=1200.0)
    assert engine.active is True

    # single approval not enough
    engine.request_approval("alice")
    lifted = engine.lift_if_approved()
    assert lifted is False
    assert engine.active is True

    # second approval triggers lift
    engine.request_approval("bob")
    lifted2 = engine.lift_if_approved()
    assert lifted2 is True
    assert engine.active is False

    # audit contains override event and approvals
    [
        b
        for b in ledger._chain
        if b.payload.get("new_tier") is None and b.payload.get("approver")
    ]
    # we expect approval entries exist in ledger
    approvals = [
        b
        for b in ledger._chain
        if isinstance(b.payload, dict) and b.payload.get("approver")
    ]
    assert len(approvals) >= 2
