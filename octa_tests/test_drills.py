from octa_tests.drills.audit_degraded import run_audit_degraded
from octa_tests.drills.correlation_drawdown import run_correlation_drawdown
from octa_tests.drills.data_integrity_failure import run_data_integrity_failure
from octa_tests.drills.execution_ack_timeout import run_execution_ack_timeout
from octa_tests.drills.helpers import AuditStub, IncidentLog
from octa_tests.drills.message_bus_backlog import run_message_bus_backlog


class SentinelMock:
    def __init__(self):
        self.last = None

    def set_gate(self, level, reason):
        self.last = (level, reason)


def test_audit_degraded_triggers_incident(tmp_path):
    audit_file = tmp_path / "audit.log"
    incident_file = tmp_path / "incidents.log"
    AuditStub(str(audit_file), fail=False)
    incident = IncidentLog(str(incident_file))
    sentinel = SentinelMock()

    # simulate failure by passing failing audit function wrapper
    def failing_audit(evt, payload):
        raise IOError("disk slow")

    res = run_audit_degraded(
        failing_audit, incident.record, sentinel, simulate_failure=True
    )
    assert res["pass"] is False
    assert sentinel.last[0] == 3
    # incident recorded
    lines = incident_file.read_text().splitlines()
    assert lines


def test_message_bus_backlog_triggers(tmp_path):
    incident_file = tmp_path / "incidents.log"
    incident = IncidentLog(str(incident_file))
    sentinel = SentinelMock()

    def backlog():
        return 500

    res = run_message_bus_backlog(
        backlog, threshold=100, incident_recorder=incident.record, sentinel_api=sentinel
    )
    assert res["pass"] is False
    assert sentinel.last[0] == 2


def test_execution_ack_timeout_triggers(tmp_path):
    incident_file = tmp_path / "incidents.log"
    incident = IncidentLog(str(incident_file))
    sentinel = SentinelMock()

    def timeouts():
        return 50

    res = run_execution_ack_timeout(
        timeouts, threshold=10, incident_recorder=incident.record, sentinel_api=sentinel
    )
    assert res["pass"] is False
    assert sentinel.last[0] == 3


def test_data_integrity_failure_triggers(tmp_path):
    incident_file = tmp_path / "incidents.log"
    incident = IncidentLog(str(incident_file))
    sentinel = SentinelMock()

    def check():
        return False

    res = run_data_integrity_failure(check, incident.record, sentinel)
    assert res["pass"] is False
    assert sentinel.last[0] == 3


def test_correlation_drawdown_drill_triggers(tmp_path):
    incident_file = tmp_path / "incidents.log"
    incident = IncidentLog(str(incident_file))
    sentinel = SentinelMock()

    def corr_fn():
        return {"score": 0.8}

    def drawdown_fn(dd):
        return {"compression": {"s:all": 0.0}, "kill": dd >= 0.1}

    res = run_correlation_drawdown(corr_fn, drawdown_fn, incident.record, sentinel)
    assert res["pass"] is False
    assert sentinel.last[0] == 3
    assert incident_file.exists()
