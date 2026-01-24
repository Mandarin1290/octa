from octa_reg.change_mgmt import ChangeManagement


def test_unapproved_change_blocked():
    cm = ChangeManagement()
    rid = cm.create_request("Update config", "Adjust parameter", proposer="dev")
    try:
        cm.apply_change(rid, actor="deployer")
        raise AssertionError("expected RuntimeError for unapproved change")
    except RuntimeError:
        pass


def test_emergency_path_audited():
    cm = ChangeManagement()
    rid = cm.create_request("Hotfix", "Fix critical bug", proposer="dev")
    cm.emergency_override(rid, actor="ops", justification="critical outage")
    # now apply should succeed
    cm.apply_change(rid, actor="ops")
    # check audit log contains emergency_override and change_applied
    actions = [e["action"] for e in cm.audit_log]
    assert "emergency_override" in actions
    assert "change_applied" in actions
