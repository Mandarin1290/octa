from octa_wargames.privilege_abuse import ConfigStore, PrivilegeManager, User


def test_unauthorized_config_change_blocked_and_logged():
    cfg = ConfigStore()
    pm = PrivilegeManager(cfg)

    user = User(id="alice", roles=["trader"])
    ok = pm.attempt_change_config(user, "max_leverage", 10)
    assert ok is False
    # find last audit entry for denial
    audits = [a for a in pm.audit if a["actor"] == "alice"]
    assert any(a["action"].endswith("denied") for a in audits)


def test_bypass_and_reallocate_require_admin_or_ops():
    cfg = ConfigStore()
    cfg.data["capital_ledger"] = {"acctA": 1000.0, "acctB": 100.0}
    pm = PrivilegeManager(cfg)

    thief = User(id="mallory", roles=["trader"])
    ok1 = pm.attempt_bypass_risk(thief, "gate-1")
    ok2 = pm.attempt_reallocate(thief, "acctA", "acctB", 500.0)
    assert ok1 is False
    assert ok2 is False
    # now an admin performs reallocation
    admin = User(id="admin", roles=["admin"])
    ok3 = pm.attempt_reallocate(admin, "acctA", "acctB", 200.0)
    assert ok3 is True
    # ledger updated and audit recorded
    assert cfg.data["capital_ledger"]["acctB"] >= 300.0
    assert any(e.action == "reallocate" for e in cfg.audit)
    # ensure pm.audit contains entries for both denied and allowed actions
    assert any(a["action"].endswith("denied") for a in pm.audit)
    assert any(a["action"].endswith("applied") for a in pm.audit)
