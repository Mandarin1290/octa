import pytest

from octa_governance.escalation import EscalationException, EscalationManager


def test_escalation_triggered_and_audited():
    mgr = EscalationManager()
    esc = mgr.trigger_escalation(
        "risk_vs_execution", {"conflict": "limit_override"}, created_by="alice"
    )
    assert esc.id in mgr._store
    # audit log contains the trigger event
    assert any(
        e["action"] == "escalation_triggered" and e["details"]["id"] == esc.id
        for e in mgr.audit_log
    )


def test_resolution_requires_multiple_approvals_and_is_logged():
    mgr = EscalationManager()
    esc = mgr.trigger_escalation(
        "audit_anomaly",
        {"anomaly": "missing_record"},
        created_by="eve",
        required_approval_count=2,
        required_roles={"audit", "ops"},
    )

    # single approval should be insufficient
    mgr.add_approval(esc.id, "alice", "audit")
    with pytest.raises(EscalationException) as ei:
        mgr.resolve_escalation(esc.id, resolved_by="bob")
    assert "insufficient_approvals" in str(ei.value) or "required_roles_missing" in str(
        ei.value
    )

    # add missing role
    mgr.add_approval(esc.id, "carol", "ops")
    mgr.resolve_escalation(esc.id, resolved_by="bob")
    assert mgr.get(esc.id).status == "resolved"
    assert any(
        e["action"] == "escalation_resolved" and e["details"]["id"] == esc.id
        for e in mgr.audit_log
    )
