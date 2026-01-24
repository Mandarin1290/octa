from octa_audit.continuous_audit import ContinuousAudit


def test_snapshot_valid_and_verifiable():
    ca = ContinuousAudit()
    components = {"ledger_hash": "abc123", "models": {"m1": "v1"}}
    sid = ca.take_snapshot("daily_snapshot", components)
    assert sid
    rec = ca.get_snapshot(sid)
    assert rec is not None
    assert rec.get("evidence_hash")
    assert ca.verify_snapshot(sid) is True


def test_snapshot_tamper_detected():
    ca = ContinuousAudit()
    components = {"ledger_hash": "orig", "state": {"a": 1}}
    sid = ca.take_snapshot("snap", components)
    rec = ca.get_snapshot(sid)
    # simulate tampering by modifying stored components without updating evidence_hash
    rec["components"]["ledger_hash"] = "tampered"
    assert ca.verify_snapshot(sid) is False
