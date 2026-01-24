from octa_ops.incidents import IncidentManager, Severity
from octa_reg.control_matrix import Control, ControlMatrix, ControlObjective
from octa_reg.dd_pack import create_dd_pack
from octa_reg.model_risk import ModelRiskManager


def test_dd_pack_reproducible_and_reconcilable():
    im = IncidentManager()
    im.record_incident(title="I1", description="d", reporter="r", severity=Severity.S1)
    im.record_incident(title="I2", description="d2", reporter="r", severity=Severity.S2)

    cm = ControlMatrix()
    obj = ControlObjective(id="OBJ-1", description="records")
    cm.register_objective(obj)
    ctrl = Control(
        id="C1",
        objective_id="OBJ-1",
        description="immutable log",
        owner="compliance",
        frequency="daily",
        testable=True,
    )
    cm.register_control(ctrl)

    mrm = ModelRiskManager()
    mid = mrm.register_model("pricing", "v1")
    mrm.add_validation_evidence(mid, {"report": "ok"}, actor="val")
    mrm.approve_model(mid, approver="gov")

    pack1 = create_dd_pack(incident_manager=im, control_matrix=cm, model_risk=mrm)
    pack2 = create_dd_pack(incident_manager=im, control_matrix=cm, model_risk=mrm)

    assert pack1.hash == pack2.hash
    # incidents count reconcile
    assert len(pack1.snapshot["incidents"]) == len(im.list_incidents())
    # control objective present
    assert "OBJ-1" in pack1.snapshot["risk_framework"]["objectives"]
