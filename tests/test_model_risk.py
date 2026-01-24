from octa_reg.model_risk import ModelRiskManager


def test_unapproved_model_blocked():
    mrm = ModelRiskManager()
    mid = mrm.register_model("alpha_model", "v1")
    try:
        mrm.use_model(mid, actor="runner")
        raise AssertionError("expected RuntimeError for unapproved model use")
    except RuntimeError:
        pass


def test_approval_audited_and_override_logged():
    mrm = ModelRiskManager()
    mid = mrm.register_model("pricing", "v0.1")
    # add validation evidence
    mrm.add_validation_evidence(
        mid, {"report": "validation_pass", "score": 0.99}, actor="validator"
    )
    mrm.approve_model(mid, approver="governance")

    # now use should be allowed
    mrm.use_model(mid, actor="prod")
    # find approval log
    approves = [
        e
        for e in mrm.audit_log
        if e["action"] == "model_approved" and e["details"]["id"] == mid
    ]
    assert len(approves) == 1

    # register another model and override use
    mid2 = mrm.register_model("experimental", "v0.0")
    # override use should be logged but not set approved
    mrm.override_use(mid2, actor="ops", justification="emergency test")
    overrides = [
        e
        for e in mrm.audit_log
        if e["action"] == "model_use_overridden" and e["details"]["id"] == mid2
    ]
    assert len(overrides) == 1
