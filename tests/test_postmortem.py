from octa_ops.postmortem import generate_postmortem


def test_incident_parsed_and_report_generated():
    incident = {
        "id": "test-1",
        "events": [
            {
                "ts": "2025-12-28T09:00:00Z",
                "message": "feed lag detected",
                "type": "warning",
            },
            {
                "ts": "2025-12-28T09:01:00Z",
                "message": "feed stale",
                "type": "error",
                "severity": "critical",
            },
        ],
        "failed_safeguards": ["stale_data"],
        "evidence": {"raw_feed": {"prices": [1, 2, 3]}},
    }

    report = generate_postmortem(incident)
    rd = report.to_dict()

    assert rd["incident_id"] == "test-1"
    assert isinstance(rd["timeline"], list) and len(rd["timeline"]) == 2
    assert rd["root_cause"] in ("stale_data", "feed stale", "undetermined")
    assert "remediation_actions" in rd and isinstance(rd["remediation_actions"], list)
    assert rd["blame_free"] is True
    assert "incident_snapshot" in rd["evidence"]


def test_root_cause_heuristics_prefers_explicit():
    incident = {"id": "test-2", "root_cause": "configuration_change", "events": []}
    report = generate_postmortem(incident)
    assert report.root_cause == "configuration_change"


from octa_ops.incidents import IncidentManager, Severity
from octa_ops.postmortem import PostmortemManager


def test_review_triggered_for_s2_incident():
    im = IncidentManager()
    inc = im.record_incident(
        title="Trade failure",
        description="failed fills",
        reporter="bot",
        severity=Severity.S2,
    )

    pm = PostmortemManager()
    review = pm.start_review(incident=inc, reviewer="alice")

    # verify review required and initial timeline contains incident_reported
    assert review.required is True
    assert any(e for e in review.timeline if e["event"] == "incident_reported")


def test_actions_tracked_and_completed():
    im = IncidentManager()
    inc = im.record_incident(
        title="Pricing error",
        description="stale price",
        reporter="monitor",
        severity=Severity.S3,
    )

    pm = PostmortemManager()
    pm.start_review(inc, reviewer="bob")
    pm.add_remediation_task(
        inc.id, task_id="T1", description="fix price feed", owner="devops"
    )
    pm.add_remediation_task(
        inc.id, task_id="T2", description="reconcile bad trades", owner="ops"
    )

    open_tasks = pm.list_open_tasks(inc.id)
    assert len(open_tasks) == 2

    pm.complete_task(inc.id, "T1", actor="devops")
    open_tasks2 = pm.list_open_tasks(inc.id)
    assert len(open_tasks2) == 1
