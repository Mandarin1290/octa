"""Tests for the governance hash-chain audit writer."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from octa.core.governance.governance_audit import (
    EVENT_EXECUTION_PREFLIGHT,
    EVENT_MODEL_PROMOTED,
    EVENT_PORTFOLIO_PREFLIGHT,
    GovernanceAudit,
)


def test_governance_audit_creates_chain(tmp_path: Path) -> None:
    ga = GovernanceAudit(run_id="test_run_001", root=tmp_path)
    assert ga.run_id == "test_run_001"
    assert ga.chain_path.parent.exists()


def test_governance_audit_emit_and_verify(tmp_path: Path) -> None:
    ga = GovernanceAudit(run_id="run_002", root=tmp_path)
    ts = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    rec1 = ga.emit(EVENT_EXECUTION_PREFLIGHT, {"mode": "dry-run"}, ts=ts)
    assert rec1.index == 1
    assert rec1.prev_hash == "GENESIS"
    assert rec1.hash

    rec2 = ga.emit(EVENT_MODEL_PROMOTED, {"model": "abc.cbm"}, ts=ts)
    assert rec2.index == 2
    assert rec2.prev_hash == rec1.hash

    assert ga.verify() is True


def test_governance_audit_reject_unknown_event(tmp_path: Path) -> None:
    ga = GovernanceAudit(run_id="run_003", root=tmp_path)
    with pytest.raises(ValueError, match="Unknown governance event type"):
        ga.emit("BOGUS_EVENT", {"data": 1})


def test_governance_audit_read_events(tmp_path: Path) -> None:
    ga = GovernanceAudit(run_id="run_004", root=tmp_path)
    ts = datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
    ga.emit(EVENT_EXECUTION_PREFLIGHT, {"a": 1}, ts=ts)
    ga.emit(EVENT_PORTFOLIO_PREFLIGHT, {"b": 2}, ts=ts)

    events = ga.read_events()
    assert len(events) == 2
    assert events[0]["payload"]["event_type"] == EVENT_EXECUTION_PREFLIGHT
    assert events[1]["payload"]["event_type"] == EVENT_PORTFOLIO_PREFLIGHT


def test_governance_audit_summary(tmp_path: Path) -> None:
    ga = GovernanceAudit(run_id="run_005", root=tmp_path)
    ts = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    ga.emit(EVENT_EXECUTION_PREFLIGHT, {}, ts=ts)
    ga.emit(EVENT_EXECUTION_PREFLIGHT, {}, ts=ts)
    ga.emit(EVENT_MODEL_PROMOTED, {"m": "x"}, ts=ts)

    s = ga.summary()
    assert s["total_events"] == 3
    assert s["integrity_ok"] is True
    assert s["event_counts"][EVENT_EXECUTION_PREFLIGHT] == 2
    assert s["event_counts"][EVENT_MODEL_PROMOTED] == 1


def test_governance_audit_tamper_detection(tmp_path: Path) -> None:
    ga = GovernanceAudit(run_id="run_006", root=tmp_path)
    ts = datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    ga.emit(EVENT_EXECUTION_PREFLIGHT, {"mode": "live"}, ts=ts)
    ga.emit(EVENT_MODEL_PROMOTED, {"model": "x.cbm"}, ts=ts)
    assert ga.verify() is True

    # Tamper with the chain
    lines = ga.chain_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    record = json.loads(lines[1])
    record["payload"]["data"]["model"] = "TAMPERED"
    lines[1] = json.dumps(record, sort_keys=True)
    ga.chain_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    assert ga.verify() is False


def test_governance_audit_empty_run_id_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="run_id must be non-empty"):
        GovernanceAudit(run_id="", root=tmp_path)


def test_governance_audit_empty_chain_verifies(tmp_path: Path) -> None:
    ga = GovernanceAudit(run_id="run_empty", root=tmp_path)
    assert ga.verify() is True
    assert ga.read_events() == []
    s = ga.summary()
    assert s["total_events"] == 0
    assert s["integrity_ok"] is True
