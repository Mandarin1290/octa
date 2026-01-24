from octa_reg.control_matrix import Control, ControlMatrix, ControlObjective


def test_missing_control_flagged():
    cm = ControlMatrix()
    obj = ControlObjective(
        id="OBJ-1", description="Ensure accurate records", domains=["record_keeping"]
    )
    cm.register_objective(obj)

    missing = cm.flag_missing_controls()
    assert "OBJ-1" in missing


def test_ownership_enforced():
    cm = ControlMatrix()
    obj = ControlObjective(
        id="OBJ-2",
        description="Prevent market abuse",
        domains=["market_abuse_prevention"],
    )
    cm.register_objective(obj)

    # register control without owner
    ctrl = Control(
        id="C-1",
        objective_id="OBJ-2",
        description="pre-trade limit check",
        owner=None,
        frequency="daily",
        testable=True,
    )
    cm.register_control(ctrl)

    missing = cm.enforce_ownership()
    assert "C-1" in missing

    try:
        cm.enforce_ownership_raise()
        raise AssertionError("expected ValueError for missing control owners")
    except ValueError:
        pass
