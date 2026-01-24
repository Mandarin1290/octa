import pytest

from octa_sentinel.live_checklist import LiveChecklist


def test_missing_item_blocks_live_flag():
    events = []

    def audit(e, p):
        return events.append((e, p))

    lc = LiveChecklist(required_shadow_days=2, audit_fn=audit, signer_key="testkey")

    # run checks with missing items
    ctx = {
        "paper_passed": True,
        "shadow_days": 1,  # insufficient
        "critical_incidents": 0,
        "audit_chain_ok": True,
        "kill_tested": True,
        "capacity_passed": True,
        "liquidity_passed": True,
    }
    res = lc.run_checks(ctx)
    assert res.passed is False
    ok = lc.enable_live()
    assert ok is False


def test_results_immutable_and_signed():
    lc = LiveChecklist(required_shadow_days=1, signer_key="k")
    ctx = {
        "paper_passed": True,
        "shadow_days": 1,
        "critical_incidents": 0,
        "audit_chain_ok": True,
        "kill_tested": True,
        "capacity_passed": True,
        "liquidity_passed": True,
    }
    res = lc.run_checks(ctx)
    assert res.passed is True
    # dataclass is frozen: mutation attempts raise
    with pytest.raises(Exception):
        res.passed = False
    # signature is present and fixed
    assert isinstance(res.signature, str) and len(res.signature) > 0
