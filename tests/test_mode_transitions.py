from octa_fabric.mode import Mode, ModeManager
from octa_governance.committee import DecisionState, GoLiveCommittee
from octa_ops.operators import Operator, OperatorRegistry, Role
from octa_sentinel.live_checklist import LiveChecklist


def test_illegal_transition_blocked():
    events = []

    def audit(e, p):
        return events.append((e, p))

    checklist = LiveChecklist(required_shadow_days=0, audit_fn=audit)
    committee = GoLiveCommittee(audit_fn=audit)
    ops = OperatorRegistry(audit_fn=audit)

    mm = ModeManager(
        audit_fn=audit,
        live_checklist=checklist,
        committee=committee,
        operator_registry=ops,
    )

    # attempt illegal direct transition DEV -> LIVE should raise
    try:
        mm.enable_live("op1", "s1", "op2", "s2")
        raise AssertionError("expected illegal transition to raise")
    except RuntimeError:
        pass


def test_audit_includes_mode_and_live_enable():
    events = []

    def audit(e, p):
        return events.append((e, p))

    checklist = LiveChecklist(required_shadow_days=0, audit_fn=audit)
    # create checklist pass
    checklist.run_checks(
        {
            "paper_passed": True,
            "shadow_days": 0,
            "critical_incidents": 0,
            "audit_chain_ok": True,
            "kill_tested": True,
            "capacity_passed": True,
            "liquidity_passed": True,
        }
    )

    committee = GoLiveCommittee(audit_fn=audit, signer_key="k")
    committee.propose_decision(
        DecisionState.APPROVED, "ok", {}, [], cooling_off_seconds=0
    )

    ops = OperatorRegistry(audit_fn=audit)
    ops.register(Operator(operator_id="em1", role=Role.EMERGENCY, key="k1"))
    ops.register(Operator(operator_id="em2", role=Role.EMERGENCY, key="k2"))

    # move to PAPER -> SHADOW
    mm = ModeManager(
        audit_fn=audit,
        live_checklist=checklist,
        committee=committee,
        operator_registry=ops,
    )
    mm.to_paper()
    mm.to_shadow()

    # prepare canonical payload and signatures
    canon = "enable_live|now"
    sig1 = ops.sign("em1", canon)
    sig2 = ops.sign("em2", canon)

    ok = mm.enable_live("em1", sig1, "em2", sig2, payload="now")
    assert ok is True
    # check audit events contain mode field
    assert any("mode" in p for (_, p) in events)
    assert mm.mode() == Mode.LIVE
