import pytest

from octa_ml.model_refresh import ModelRefreshManager


def test_unauthorized_retrain_blocked():
    mgr = ModelRefreshManager()
    mgr.add_model("m1", "v1")
    # request but do not approve
    mgr.request_retrain("m1", trigger="data_drift", proposer="mlops")
    with pytest.raises(PermissionError):
        mgr.execute_retrain("m1", "v2", validate_metrics={"pass": True})


def test_approval_enforced_and_execute():
    mgr = ModelRefreshManager()
    mgr.add_model("m2", "v1")
    mgr.request_retrain("m2", trigger="data_refresh", proposer="mlops")
    mgr.approve_retrain("m2", approver="governance-committee")
    evidence = mgr.execute_retrain("m2", "v2", validate_metrics={"pass": True})
    assert isinstance(evidence, str) and len(evidence) == 64
    assert mgr.get_current_version("m2") == "v2"

    # rollback should be available and revert
    rb = mgr.rollback("m2")
    assert isinstance(rb, str) and len(rb) == 64
    assert mgr.get_current_version("m2") == "v1"
