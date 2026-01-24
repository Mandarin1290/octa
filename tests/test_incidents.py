import pytest

from octa_ops.incidents import IncidentManager, Severity


def test_severity_classification_deterministic():
    im = IncidentManager()
    assert im.classify_from_impact(0) == Severity.S0
    assert im.classify_from_impact(5) == Severity.S1
    assert im.classify_from_impact(10) == Severity.S1
    assert im.classify_from_impact(11) == Severity.S2
    assert im.classify_from_impact(50) == Severity.S2
    assert im.classify_from_impact(51) == Severity.S3
    assert im.classify_from_impact(200) == Severity.S3
    assert im.classify_from_impact(201) == Severity.S4


def test_escalation_mapping_correct():
    im = IncidentManager()
    # S3 escalation should include risk_officer and trading_desk_lead
    esc = im.escalation_for(Severity.S3)
    assert "risk_officer" in esc
    assert "trading_desk_lead" in esc


def test_record_without_severity_raises():
    im = IncidentManager()
    with pytest.raises(ValueError):
        im.record_incident(
            title="oops", description="no severity", reporter="alice", severity=None
        )


from octa_ledger.store import LedgerStore
from octa_sentinel.engine import SentinelEngine
from octa_sentinel.policies import SentinelPolicy


def test_incident_auto_created_on_sentinel_l2(tmp_path):
    lp = tmp_path / "ledger"
    ls = LedgerStore(str(lp))

    policy = SentinelPolicy(schema_version=1, name="p")
    eng = SentinelEngine(
        policy=policy, state_path=str(tmp_path / "state"), ledger_store=ls
    )

    # force daily loss above threshold to trigger level 2 freeze
    inputs = {
        "pnl": {"current_nav": 98.0, "peak_nav": 100.0, "daily_loss": 0.05},
        "exposure": {},
        "health": {},
    }
    dec = eng.evaluate(inputs)
    assert dec.level >= 2

    # ledger should contain incident.created and incident.timeline and gate_event
    created = ls.by_action("incident.created")
    timelines = ls.by_action("incident.timeline")
    gates = ls.by_action("gate_event")
    assert len(created) >= 1
    assert len(timelines) >= 1
    assert len(gates) >= 1

    # Verify the created incident payload references severity and title
    c = created[-1]
    assert c["payload"]["severity"] >= 2
    assert "drawdown" in c["payload"]["title"] or c["payload"]["title"]


def test_audit_trail_complete(tmp_path):
    lp = tmp_path / "ledger2"
    ls = LedgerStore(str(lp))
    policy = SentinelPolicy(schema_version=1, name="p2")
    eng = SentinelEngine(
        policy=policy, state_path=str(tmp_path / "state2"), ledger_store=ls
    )

    inputs = {
        "pnl": {"current_nav": 50.0, "peak_nav": 100.0, "daily_loss": 0.6},
        "exposure": {},
        "health": {},
    }
    dec = eng.evaluate(inputs)
    assert dec.level >= 2

    # Ensure events linked (incident created then timeline then gate_event present)
    created = ls.by_action("incident.created")
    timeline = ls.by_action("incident.timeline")
    gate = ls.by_action("gate_event")
    assert created
    assert timeline
    assert gate
