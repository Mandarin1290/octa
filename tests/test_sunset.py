import pytest

from octa_strategy.sunset import SunsetEngine


def test_orderly_shutdown_and_capital_reclaimed():
    eng = SunsetEngine()
    eng.add_strategy("alpha", 1_500_000.0)
    assert eng.get_state("alpha") == "active"
    assert eng.get_capital("alpha") == 1_500_000.0

    # trigger sunset via alpha_decay
    eng.initiate_sunset("alpha", trigger="alpha_decay", notes="decay observed")
    assert eng.get_state("alpha") == "sunset"

    reclaimed = eng.perform_shutdown("alpha")
    assert reclaimed == pytest.approx(1_500_000.0)
    assert eng.get_state("alpha") == "retired"
    assert eng.get_capital("alpha") == 0.0

    audit = eng.get_audit()
    assert any(e["action"] == "sunset_initiated" for e in audit)
    assert any(e["action"] == "shutdown_complete" for e in audit)


def test_reinstatement_requires_governance():
    eng = SunsetEngine()
    eng.add_strategy("beta", 500_000.0)
    eng.initiate_sunset("beta", trigger="capacity_breach")
    eng.perform_shutdown("beta")

    with pytest.raises(PermissionError):
        eng.reinstate("beta", governance_approval=False)

    # governance approved reinstatement works
    evidence = eng.reinstate(
        "beta", governance_approval=True, approver="governance-committee"
    )
    assert isinstance(evidence, str) and len(evidence) == 64
    assert eng.get_state("beta") == "active"
