import pytest

from octa_core.live_gates import LiveGateFailure, LiveGates


def test_gate_enforcement_all_pass():
    lg = LiveGates()
    risk_metrics = {"max_drawdown": 0.02}
    thresholds = {"max_drawdown": 0.1}
    execution = {
        "connected": True,
        "latency_ms": 100,
        "latency_threshold_ms": 500,
        "failure_rate": 0.0,
        "failure_rate_threshold": 0.01,
    }
    data_checks = {"price_feed": {"last_update_age_s": 1, "max_age_s": 5}}
    governance = {
        "approvals": ["alice", "bob"],
        "approved_roles": ["risk", "ops"],
        "required_count": 2,
        "required_roles": ["risk", "ops"],
    }

    assert (
        lg.enforce_live(
            risk_metrics, thresholds, execution, data_checks, governance, actor="tester"
        )
        is True
    )
    assert any(e["action"] == "live_gates_passed" for e in lg.audit_log)


def test_gate_rejection_correctness():
    lg = LiveGates()
    # risk metric exceeds threshold
    risk_metrics = {"max_drawdown": 0.5}
    thresholds = {"max_drawdown": 0.1}
    execution = {
        "connected": True,
        "latency_ms": 100,
        "latency_threshold_ms": 500,
        "failure_rate": 0.0,
        "failure_rate_threshold": 0.01,
    }
    data_checks = {"price_feed": {"last_update_age_s": 1, "max_age_s": 5}}
    governance = {
        "approvals": ["alice"],
        "approved_roles": ["risk"],
        "required_count": 2,
        "required_roles": ["risk", "ops"],
    }

    with pytest.raises(LiveGateFailure) as ei:
        lg.enforce_live(
            risk_metrics, thresholds, execution, data_checks, governance, actor="tester"
        )

    # ensure audit recorded blocked event
    assert any(e["action"] == "live_gates_blocked" for e in lg.audit_log)
    details = ei.value.details
    assert "failed_gates" in details and "risk_metrics" in details["failed_gates"]
