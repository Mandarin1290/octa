import pytest

from octa_ops.runbooks import Runbook, RunbookError, RunbookManager


def sample_runbook():
    return Runbook(
        incident_type="data_feed_stale",
        summary="Price feed stale",
        immediate_actions=[
            {
                "action_type": "command",
                "command": "restart_data_feed",
                "expected_result": "service_running",
            }
        ],
        escalation_chain=[{"role": "ops", "contact": "ops@org"}],
        recovery_steps=[
            {
                "action_type": "command",
                "command": "switch_to_fallback_feed",
                "expected_result": "fallback_active",
            }
        ],
    )


def test_runbook_completeness_and_validation():
    rb = sample_runbook()
    # validation should not raise
    rb.validate()
    mgr = RunbookManager()
    mgr.add_runbook(rb, actor="test")
    assert rb.incident_type in mgr._store
    assert any(e["action"] == "runbook_added" for e in mgr.audit_log)


def test_incident_mapping_and_execution_audit():
    rb = sample_runbook()
    mgr = RunbookManager()
    mgr.add_runbook(rb, actor="test")

    # retrieve by incident type
    got = mgr.get_runbook("data_feed_stale")
    assert got.id == rb.id

    # execute (simulate) and ensure audit recorded
    exec_rec = mgr.execute_runbook("data_feed_stale", actor="ops_sim", simulate=True)
    assert exec_rec["incident_type"] == "data_feed_stale"
    assert any(e["action"] == "runbook_executed" for e in mgr.audit_log)

    # missing runbook raises
    with pytest.raises(RunbookError):
        mgr.get_runbook("unknown_incident")


from octa_ops.library import broker_runbook as broker
from octa_ops.library import datafeed_runbook as df
from octa_ops.runbooks import RunbookEngine, StepResult


class SentinelMock:
    def __init__(self):
        self.calls = []

    def set_gate(self, level, reason):
        self.calls.append((level, reason))


def test_runbook_executes_stepwise():
    events = []

    def audit(e, p):
        events.append((e, p))

    sentinel = SentinelMock()
    engine = RunbookEngine(
        audit_fn=audit,
        sentinel_api=sentinel,
        incident_fn=lambda i: events.append(("incident", i)),
    )

    # register a simple broker disconnect runbook
    engine.register(
        "broker_disconnect",
        [broker.step_notify_broker_disconnect, broker.step_reconnect_broker],
    )

    ctx = {"notify_ops": lambda: True, "reconnect": lambda: True}
    results = engine.execute("broker_disconnect", ctx)
    assert isinstance(results, list)
    assert all(isinstance(r, StepResult) for r in results)
    assert len(results) == 2


def test_failure_escalates_incident():
    events = []

    def audit(e, p):
        events.append((e, p))

    sentinel = SentinelMock()
    engine = RunbookEngine(
        audit_fn=audit,
        sentinel_api=sentinel,
        incident_fn=lambda i: events.append(("incident", i)),
    )

    # register datafeed runbook where verify_feed fails
    engine.register(
        "datafeed_outage", [df.step_verify_feed, df.step_switch_to_failover]
    )
    ctx = {"feed_ok": False, "switch_failover": lambda: True}
    engine.execute("datafeed_outage", ctx)
    # should stop at first failed step and have incident recorded
    assert any(e[0] == "incident" for e in events)
    assert any(c[0] == 3 or c[0] == 2 for c in sentinel.calls)
