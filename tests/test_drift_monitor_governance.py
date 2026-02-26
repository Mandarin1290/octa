"""Tests for I4: drift breach events emitted to governance hash-chain.

All tests are offline-safe (tmp_path, no network, no broker).
Uses monkeypatch.chdir(tmp_path) to isolate drift state writes from global state.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from octa_ledger.events import AuditEvent
from octa_ledger.store import LedgerStore

from octa.core.governance.drift_monitor import evaluate_drift
from octa.core.governance.governance_audit import EVENT_DRIFT_BREACH, GovernanceAudit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BREACH_CFG = {"kpi_threshold": 0.0, "window_days": 20, "breach_days": 2}
_MODEL_KEY = "TEST_DG_1D"


def _make_ledger(tmp_path, nav_decay: float = 0.999, days: int = 25) -> str:
    """Build a ledger with steady NAV decay (→ negative Sharpe → breach)."""
    ledger_dir = tmp_path / "ledger"
    ledger = LedgerStore(str(ledger_dir))
    start = datetime.now(timezone.utc) - timedelta(days=days + 5)
    nav = 100.0
    for i in range(days):
        nav *= nav_decay
        ts = (start + timedelta(days=i)).isoformat()
        ev = AuditEvent.create(
            actor="test",
            action="performance.nav",
            payload={"date": ts, "nav": nav},
            severity="INFO",
        )
        ledger.append(ev)
    return str(ledger_dir)


def _make_ledger_positive(tmp_path) -> str:
    """Build a ledger with NAV growth (→ positive Sharpe → no breach)."""
    return _make_ledger(tmp_path, nav_decay=1.005)


def _make_gov(tmp_path, run_id: str = "test_i4") -> GovernanceAudit:
    return GovernanceAudit(run_id=run_id, root=tmp_path / "audit")


def _event_types(gov: GovernanceAudit) -> list:
    return [ev["payload"]["event_type"] for ev in gov.read_events()]


def _call_drift(ledger_dir: str, gov=None, ctx=None) -> object:
    return evaluate_drift(
        ledger_dir=ledger_dir,
        model_key=_MODEL_KEY,
        gate="global_1d",
        timeframe="1D",
        bucket="default",
        cfg=_BREACH_CFG,
        gov_audit=gov,
        ctx=ctx,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_drift_breach_emits_governance_event(tmp_path, monkeypatch):
    """A streak-2 breach must produce one EVENT_DRIFT_BREACH in the audit chain."""
    monkeypatch.chdir(tmp_path)
    ledger_dir = _make_ledger(tmp_path)
    gov = _make_gov(tmp_path)

    # First call: streak=1 (below breach_days=2) → no event yet
    _call_drift(ledger_dir, gov)
    assert _event_types(gov) == []

    # Second call: streak=2 >= breach_days=2 → event emitted
    decision = _call_drift(ledger_dir, gov)
    assert decision.disabled is True
    events = _event_types(gov)
    assert EVENT_DRIFT_BREACH in events
    assert events.count(EVENT_DRIFT_BREACH) == 1


def test_no_breach_no_governance_event(tmp_path, monkeypatch):
    """Positive NAV growth → no breach → no EVENT_DRIFT_BREACH emitted."""
    monkeypatch.chdir(tmp_path)
    ledger_dir = _make_ledger_positive(tmp_path)
    gov = _make_gov(tmp_path)

    _call_drift(ledger_dir, gov)
    _call_drift(ledger_dir, gov)
    assert _event_types(gov) == []


def test_governance_event_without_gov_audit_no_crash(tmp_path, monkeypatch):
    """evaluate_drift with gov_audit=None must not raise even on breach."""
    monkeypatch.chdir(tmp_path)
    ledger_dir = _make_ledger(tmp_path)

    _call_drift(ledger_dir, gov=None)
    decision = _call_drift(ledger_dir, gov=None)
    assert decision.disabled is True  # breach still detected, no crash


def test_governance_event_payload_fields(tmp_path, monkeypatch):
    """Emitted EVENT_DRIFT_BREACH payload must include required diagnostic fields."""
    monkeypatch.chdir(tmp_path)
    ledger_dir = _make_ledger(tmp_path)
    gov = _make_gov(tmp_path)

    _call_drift(ledger_dir, gov)
    _call_drift(ledger_dir, gov)

    events = gov.read_events()
    drift_events = [
        ev for ev in events if ev["payload"]["event_type"] == EVENT_DRIFT_BREACH
    ]
    assert len(drift_events) == 1
    data = drift_events[0]["payload"]["data"]
    assert data["model_key"] == _MODEL_KEY
    assert data["timeframe"] == "1D"
    assert data["bucket"] == "default"
    assert "kpi" in data
    assert "streak" in data
    assert data["streak"] >= 2
    assert "threshold" in data
    assert "reason" in data
    assert "write_blocked" in data


def test_governance_chain_integrity_after_breach(tmp_path, monkeypatch):
    """After emitting drift events, governance chain verify() must return True."""
    monkeypatch.chdir(tmp_path)
    ledger_dir = _make_ledger(tmp_path)
    gov = _make_gov(tmp_path)

    _call_drift(ledger_dir, gov)
    _call_drift(ledger_dir, gov)

    assert gov.verify() is True


def test_write_blocked_still_emits_governance_event(tmp_path, monkeypatch):
    """Even when immutability blocks the drift state write, the governance event fires.

    The governance audit hash-chain is the audit trail — it is NOT subject to the
    immutability guard that protects registry writes.
    """
    monkeypatch.chdir(tmp_path)
    ledger_dir = _make_ledger(tmp_path)
    gov = _make_gov(tmp_path)

    # First call: build up streak=1
    _call_drift(ledger_dir, gov)

    # Second call with production context that would block state writes
    prod_ctx = {
        "mode": "paper",
        "service": "test",
        "execution_active": True,
        "run_id": "exec_test",
        "entrypoint": "execution_service",
    }
    decision = evaluate_drift(
        ledger_dir=ledger_dir,
        model_key=_MODEL_KEY,
        gate="global_1d",
        timeframe="1D",
        bucket="default",
        cfg=_BREACH_CFG,
        ctx=prod_ctx,
        gov_audit=gov,
    )

    # State write is blocked in production context
    assert decision.reason == "drift_write_blocked"
    assert decision.diagnostics["state_write_blocked"] is True

    # But governance event is still emitted
    events = _event_types(gov)
    assert EVENT_DRIFT_BREACH in events

    # And the event records write_blocked=True
    drift_events = [
        ev for ev in gov.read_events()
        if ev["payload"]["event_type"] == EVENT_DRIFT_BREACH
    ]
    assert drift_events[0]["payload"]["data"]["write_blocked"] is True


def test_multiple_breaches_emit_multiple_events(tmp_path, monkeypatch):
    """Each call that triggers a new breach emits its own governance event."""
    monkeypatch.chdir(tmp_path)
    ledger_dir = _make_ledger(tmp_path)
    gov = _make_gov(tmp_path)

    # Call 1: streak=1 (no event)
    _call_drift(ledger_dir, gov)
    # Call 2: streak=2 → breach (event 1)
    _call_drift(ledger_dir, gov)
    # Call 3: streak=3 → still disabled, breach again (event 2)
    _call_drift(ledger_dir, gov)

    events = [e for e in _event_types(gov) if e == EVENT_DRIFT_BREACH]
    assert len(events) == 2


def test_breach_reason_in_governance_event_reflects_write_blocked(tmp_path, monkeypatch):
    """The 'reason' field in the governance event payload matches the DriftDecision.reason."""
    monkeypatch.chdir(tmp_path)
    ledger_dir = _make_ledger(tmp_path)
    gov = _make_gov(tmp_path)

    _call_drift(ledger_dir, gov)
    decision = _call_drift(ledger_dir, gov)

    assert decision.reason == "drift_breach"

    drift_events = [
        ev for ev in gov.read_events()
        if ev["payload"]["event_type"] == EVENT_DRIFT_BREACH
    ]
    assert drift_events[0]["payload"]["data"]["reason"] == "drift_breach"
    assert drift_events[0]["payload"]["data"]["write_blocked"] is False
