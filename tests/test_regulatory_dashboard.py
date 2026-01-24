from octa_ops.postmortem import PostmortemManager
from octa_reg.audit_evidence import create_evidence
from octa_reg.change_mgmt import ChangeManagement
from octa_reg.control_matrix import Control, ControlMatrix, ControlObjective
from octa_reg.model_risk import ModelRiskManager
from octa_reports.regulatory_dashboard import RegulatoryDashboard


def test_dashboard_reconciles_with_controls_and_evidence():
    cm = ControlMatrix()
    obj = ControlObjective(id="OBJ-1", description="record keeping")
    cm.register_objective(obj)
    ctrl = Control(
        id="C-1",
        objective_id="OBJ-1",
        description="immutable_logs",
        owner="compliance",
        frequency="daily",
        testable=True,
    )
    cm.register_control(ctrl)
    # attach evidence via audit evidence
    state = {"positions": {"A": 1}}
    e = create_evidence(state, ["C-1"])

    mrm = ModelRiskManager()
    mid = mrm.register_model("m", "v1")
    mrm.add_validation_evidence(mid, {"r": "ok"}, actor="v")
    mrm.approve_model(mid, approver="gov")

    cmgmt = ChangeManagement()
    rid = cmgmt.create_request("chg", "desc", proposer="dev")
    cmgmt.emergency_override(rid, actor="ops", justification="urgent")

    pm = PostmortemManager()
    # create a fake review with an open task
    # use IncidentManager to create incident
    from octa_ops.incidents import IncidentManager, Severity

    im = IncidentManager()
    inc = im.record_incident(
        title="X", description="d", reporter="r", severity=Severity.S2
    )
    pm.start_review(inc, reviewer="alice")
    pm.add_remediation_task(inc.id, task_id="T1", description="fix", owner="devops")

    dashboard = RegulatoryDashboard(
        control_matrix=cm,
        evidence_store=[e],
        postmortem_manager=pm,
        model_risk=mrm,
        change_mgmt=cmgmt,
    )
    snap = dashboard.snapshot()

    # control coverage includes OBJ-1 and control C-1
    assert "OBJ-1" in snap["control_coverage"]["objectives"]
    assert "C-1" in [c["id"] for c in snap["control_coverage"]["controls"]["OBJ-1"]]
    # evidence status maps C-1 to evidence id
    assert "C-1" in snap["evidence_status"]["evidence_by_control"]
    # open findings present
    assert len(snap["open_findings"]) >= 1
    # model approvals include our model
    assert any(
        m for m in snap["model_approvals"]["models"] if m["id"] == mid and m["approved"]
    )
    # change activity includes our request
    assert any(r for r in snap["change_activity"]["requests"] if r["id"] == rid)
