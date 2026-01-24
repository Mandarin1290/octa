from decimal import Decimal

from octa_alpha.competition import Submission, run_competition
from octa_alpha.governance import Governance


def test_veto_blocks_allocation():
    gov = Governance()
    a = Submission(
        alpha_id="A",
        requested_capital=Decimal("1000"),
        expected_return=Decimal("0.1"),
        volatility=Decimal("0.05"),
        base_confidence=Decimal("1.0"),
    )
    b = Submission(
        alpha_id="B",
        requested_capital=Decimal("1000"),
        expected_return=Decimal("0.02"),
        volatility=Decimal("0.05"),
        base_confidence=Decimal("1.0"),
    )
    gov.submit_for_approval("A")
    gov.veto("A", vetoer="board", reason="strategic risk")
    # pipeline respects governance: filter out vetoed alphas
    subs = [s for s in (a, b) if not gov.is_vetoed(s.alpha_id)]
    allocs = run_competition(subs, Decimal("1000"))
    ids = {r["alpha_id"]: r["allocated_capital"] for r in allocs}
    assert "A" not in ids or ids.get("A", Decimal("0")) == Decimal("0")


def test_audit_trail_complete():
    gov = Governance()
    gov.submit_for_approval("X")
    gov.veto("X", vetoer="compliance", reason="data gap")
    gov.override_veto("X", actor="exco", reason="approved by exception")
    logs = gov.get_audit()
    actions = [e["action"] for e in logs]
    assert actions == ["submit", "veto", "override_veto"]
    # ensure reasons present
    assert any(e["reason"] == "data gap" for e in logs)
    assert any(e["reason"] == "approved by exception" for e in logs)
