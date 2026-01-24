from octa_reg.change_mgmt import ChangeManagement
from octa_reg.control_matrix import Control, ControlMatrix, ControlObjective
from octa_reg.reg_scenarios import RegulatorySimulator


def test_gaps_detected_when_controls_missing():
    sim = RegulatorySimulator()
    cm = ControlMatrix()
    # no controls registered
    report = sim.simulate_rir(cm)
    assert "missing_record_keeping_controls" in report.gaps


def test_controls_mapped_when_present():
    sim = RegulatorySimulator()
    cm = ControlMatrix()
    obj = ControlObjective(id="OBJ-RK", description="record keeping")
    cm.register_objective(obj)
    ctrl = Control(
        id="C-RK",
        objective_id="OBJ-RK",
        description="immutable_logs",
        owner="compliance",
        frequency="daily",
    )
    cm.register_control(ctrl)

    report = sim.simulate_rir(cm)
    assert report.gaps == []
    assert "C-RK" in report.mapped_controls["record_keeping"]


def test_sudden_rule_change_checks_change_mgmt():
    sim = RegulatorySimulator()
    cm = ControlMatrix()
    cm.register_objective(ControlObjective(id="OBJ-RM", description="risk"))
    cm.register_control(
        Control(
            id="C-RM",
            objective_id="OBJ-RM",
            description="limit_monitoring",
            owner="risk",
            frequency="daily",
        )
    )

    # without change_mgmt -> gap
    report1 = sim.simulate_sudden_rule_change(cm, change_mgmt=None)
    assert "no_change_mgmt" in report1.gaps

    # with change_mgmt -> no gap for change_mgmt
    cmgmt = ChangeManagement()
    report2 = sim.simulate_sudden_rule_change(cm, change_mgmt=cmgmt)
    assert "no_change_mgmt" not in report2.gaps
