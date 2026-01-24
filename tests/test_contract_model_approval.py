from octa_ml.model_refresh import ModelRefreshManager


def test_model_retrain_requires_approval_and_records_evidence():
    mgr = ModelRefreshManager()
    model_id = "m1"

    mgr.add_model(model_id, "v1")

    # request retrain -> pending (trigger provided)
    req_evidence = mgr.request_retrain(
        model_id, trigger="scheduled", proposer="proposer"
    )
    assert isinstance(req_evidence, str)

    # executing without approval should raise
    try:
        mgr.execute_retrain(model_id, new_version="v2", validate_metrics={"pass": True})
        raised = False
    except PermissionError:
        raised = True
    assert raised

    # approve and execute with failing metrics -> should raise RuntimeError
    mgr.approve_retrain(model_id, approver="governance")
    try:
        mgr.execute_retrain(
            model_id, new_version="v2", validate_metrics={"pass": False}
        )
        executed = True
    except RuntimeError:
        executed = False
    assert not executed

    # request, approve and execute with passing metrics -> produces evidence hash
    mgr.request_retrain(model_id, trigger="ad_hoc", proposer="proposer2")
    mgr.approve_retrain(model_id, approver="governance")
    evidence = mgr.execute_retrain(
        model_id, new_version="v2", validate_metrics={"pass": True}
    )
    assert evidence and isinstance(evidence, str)
