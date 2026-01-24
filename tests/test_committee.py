from octa_governance.committee import DecisionState, GoLiveCommittee


def test_no_approval_no_live():
    events = []

    def audit(e, p):
        return events.append((e, p))

    com = GoLiveCommittee(audit_fn=audit, signer_key="k")
    assert com.decision() is None
    assert com.is_live_authorized() is False


def test_decision_logged_and_enforced():
    events = []

    def audit(e, p):
        return events.append((e, p))

    com = GoLiveCommittee(audit_fn=audit, signer_key="k")

    inputs = {"checklist_passed": True}
    attestations = [{"operator": "op1", "sig": "s1"}]
    dec = com.propose_decision(
        DecisionState.APPROVED, "all green", inputs, attestations, cooling_off_seconds=0
    )
    assert dec is not None
    # decision is immutable: second propose raises
    try:
        com.propose_decision(DecisionState.REJECTED, "oops", {}, [], 0)
        raise AssertionError("expected immutable decision to raise")
    except RuntimeError:
        pass

    assert com.decision() is not None
    assert com.is_live_authorized() is True
    # audit recorded
    assert any(e[0] == "committee_decision" for e in events)
