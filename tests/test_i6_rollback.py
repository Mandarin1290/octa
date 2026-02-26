"""I6: Rollback System — auto-demote on drift breach.

Tests that execute_rollback():
- Retires the current approved model to a _retired/ subdirectory.
- Copies the champion model files to the approved directory.
- Emits EVENT_GOVERNANCE_ENFORCED with reason="drift_rollback".
- Fails closed when champion.json is missing.
- Fails closed when the champion model files are missing.
- Updates registry lifecycle status (RETIRED / LIVE) when registry is provided.
- Verifies champion signature when public_key_path is provided.
- save_champion_record() writes a valid champion.json.

Tests for the drift_monitor trigger:
- _trigger_rollback() called on drift breach in evaluate_drift().
- Best-effort: missing champion does not raise from evaluate_drift().
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from octa.core.governance.artifact_signing import generate_keypair, sign_artifact
from octa.models.ops.rollback import (
    RollbackResult,
    execute_rollback,
    save_champion_record,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_model_dir(directory: Path, content: bytes = b"fake model bytes") -> None:
    """Populate a directory with the approved model layout."""
    directory.mkdir(parents=True, exist_ok=True)
    model = directory / "model.cbm"
    model.write_bytes(content)
    (directory / "model.cbm.sha256").write_text("deadbeef  model.cbm\n", encoding="utf-8")
    (directory / "model.cbm.sig").write_text("fakesig==\n", encoding="utf-8")
    (directory / "manifest.json").write_text(
        json.dumps({"symbol": "AAPL", "timeframe": "1D", "sha256": "deadbeef"}),
        encoding="utf-8",
    )


def _write_champion_json(path: Path, champion_model_dir: Path, artifact_id: int = 99) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({
            "symbol": "AAPL",
            "timeframe": "1D",
            "champion_model_dir": str(champion_model_dir),
            "artifact_id": artifact_id,
            "sha256": "deadbeef",
            "set_at": "2026-01-01T00:00:00Z",
        }, indent=2),
        encoding="utf-8",
    )


# ── execute_rollback() unit tests ─────────────────────────────────────────────

def test_rollback_champion_json_missing_fails_closed(tmp_path: Path) -> None:
    """No champion.json → ok=False, approved model untouched."""
    approved = tmp_path / "approved"
    current_dir = approved / "AAPL" / "1D"
    _make_model_dir(current_dir)

    result = execute_rollback(
        symbol="AAPL",
        timeframe="1D",
        champion_json_path=tmp_path / "nonexistent" / "champion.json",
        approved_root=approved,
    )

    assert result.ok is False
    assert result.reason == "champion_json_missing_or_corrupt"
    # Current model must be untouched
    assert (current_dir / "model.cbm").exists()


def test_rollback_champion_model_missing_fails_closed(tmp_path: Path) -> None:
    """champion.json exists but champion model files are absent → ok=False."""
    approved = tmp_path / "approved"
    current_dir = approved / "AAPL" / "1D"
    _make_model_dir(current_dir)

    champion_dir = tmp_path / "champion"
    champion_dir.mkdir()  # empty — no model.cbm

    champion_json = tmp_path / "champion.json"
    _write_champion_json(champion_json, champion_dir)

    result = execute_rollback(
        symbol="AAPL",
        timeframe="1D",
        champion_json_path=champion_json,
        approved_root=approved,
    )

    assert result.ok is False
    assert result.reason == "champion_model_missing"
    # Current model still in place
    assert (current_dir / "model.cbm").exists()


def test_rollback_retires_current_model(tmp_path: Path) -> None:
    """Successful rollback moves current approved files to _retired/."""
    approved = tmp_path / "approved"
    current_dir = approved / "AAPL" / "1D"
    _make_model_dir(current_dir, content=b"current model")

    champion_dir = tmp_path / "champion"
    _make_model_dir(champion_dir, content=b"champion model")

    champion_json = tmp_path / "champion.json"
    _write_champion_json(champion_json, champion_dir)

    result = execute_rollback(
        symbol="AAPL",
        timeframe="1D",
        champion_json_path=champion_json,
        approved_root=approved,
    )

    assert result.ok is True
    assert result.reason == "rollback_complete"
    # Retired path exists and contains the old model
    assert result.retired_path is not None
    assert result.retired_path.exists()
    assert (result.retired_path / "model.cbm").exists()
    assert (result.retired_path / "model.cbm").read_bytes() == b"current model"


def test_rollback_loads_champion(tmp_path: Path) -> None:
    """Successful rollback copies champion files to approved directory."""
    approved = tmp_path / "approved"
    current_dir = approved / "AAPL" / "1D"
    _make_model_dir(current_dir, content=b"current model")

    champion_dir = tmp_path / "champion"
    _make_model_dir(champion_dir, content=b"champion model bytes")

    champion_json = tmp_path / "champion.json"
    _write_champion_json(champion_json, champion_dir)

    result = execute_rollback(
        symbol="AAPL",
        timeframe="1D",
        champion_json_path=champion_json,
        approved_root=approved,
    )

    assert result.ok is True
    # Approved directory now has champion files
    assert (current_dir / "model.cbm").read_bytes() == b"champion model bytes"
    assert (current_dir / "manifest.json").exists()


def test_rollback_emits_governance_event(tmp_path: Path) -> None:
    """Rollback emits EVENT_GOVERNANCE_ENFORCED with reason=drift_rollback."""
    from octa.core.governance.governance_audit import GovernanceAudit, EVENT_GOVERNANCE_ENFORCED

    approved = tmp_path / "approved"
    current_dir = approved / "AAPL" / "1D"
    _make_model_dir(current_dir)

    champion_dir = tmp_path / "champion"
    _make_model_dir(champion_dir)

    champion_json = tmp_path / "champion.json"
    _write_champion_json(champion_json, champion_dir)

    audit = GovernanceAudit(run_id="i6_rollback_test", root=tmp_path / "audit")

    result = execute_rollback(
        symbol="AAPL",
        timeframe="1D",
        champion_json_path=champion_json,
        approved_root=approved,
        audit=audit,
    )

    assert result.ok is True

    chain_path = tmp_path / "audit" / "i6_rollback_test" / "chain.jsonl"
    assert chain_path.exists()
    records = [json.loads(line) for line in chain_path.read_text().splitlines() if line.strip()]
    enforced = [r for r in records if r["payload"]["event_type"] == "GOVERNANCE_ENFORCED"]
    assert len(enforced) == 1
    payload_data = enforced[0]["payload"]["data"]
    assert payload_data["reason"] == "drift_rollback"
    assert payload_data["symbol"] == "AAPL"
    assert payload_data["timeframe"] == "1D"


def test_rollback_no_audit_still_succeeds(tmp_path: Path) -> None:
    """Rollback works without an audit object (audit=None)."""
    approved = tmp_path / "approved"
    current_dir = approved / "AAPL" / "1D"
    _make_model_dir(current_dir)

    champion_dir = tmp_path / "champion"
    _make_model_dir(champion_dir)

    champion_json = tmp_path / "champion.json"
    _write_champion_json(champion_json, champion_dir)

    result = execute_rollback(
        symbol="AAPL",
        timeframe="1D",
        champion_json_path=champion_json,
        approved_root=approved,
        audit=None,
    )

    assert result.ok is True


def test_rollback_updates_registry_lifecycle(tmp_path: Path) -> None:
    """When registry is provided, current→RETIRED and champion→LIVE."""
    approved = tmp_path / "approved"
    _make_model_dir(approved / "AAPL" / "1D")

    champion_dir = tmp_path / "champion"
    _make_model_dir(champion_dir)

    champion_json = tmp_path / "champion.json"
    _write_champion_json(champion_json, champion_dir, artifact_id=42)

    registry = MagicMock()

    result = execute_rollback(
        symbol="AAPL",
        timeframe="1D",
        champion_json_path=champion_json,
        approved_root=approved,
        registry=registry,
        artifact_id=7,
    )

    assert result.ok is True
    registry.set_lifecycle_status.assert_any_call(7, "RETIRED")
    registry.set_lifecycle_status.assert_any_call(42, "LIVE")


def test_rollback_invalid_signature_fails_closed(tmp_path: Path) -> None:
    """Champion with invalid signature → ok=False, current model untouched."""
    priv = tmp_path / "keys" / "signing.key"
    pub = tmp_path / "keys" / "verify.pub"
    generate_keypair(priv, pub)

    approved = tmp_path / "approved"
    current_dir = approved / "AAPL" / "1D"
    _make_model_dir(current_dir, content=b"current model")

    # Champion dir has bogus .sig file
    champion_dir = tmp_path / "champion"
    _make_model_dir(champion_dir, content=b"champion model")
    # Overwrite sig with garbage to trigger signature failure
    (champion_dir / "model.cbm.sig").write_text("INVALIDSIG==\n", encoding="utf-8")

    champion_json = tmp_path / "champion.json"
    _write_champion_json(champion_json, champion_dir)

    result = execute_rollback(
        symbol="AAPL",
        timeframe="1D",
        champion_json_path=champion_json,
        approved_root=approved,
        public_key_path=pub,
    )

    assert result.ok is False
    assert result.reason == "champion_signature_invalid"
    # Current model must still be in place
    assert (current_dir / "model.cbm").read_bytes() == b"current model"


def test_rollback_valid_signature_passes(tmp_path: Path) -> None:
    """Champion with valid Ed25519 signature → rollback succeeds."""
    priv = tmp_path / "keys" / "signing.key"
    pub = tmp_path / "keys" / "verify.pub"
    generate_keypair(priv, pub)

    approved = tmp_path / "approved"
    _make_model_dir(approved / "AAPL" / "1D")

    champion_dir = tmp_path / "champion"
    champion_dir.mkdir()
    model = champion_dir / "model.cbm"
    model.write_bytes(b"signed champion model")
    sign_artifact(model, priv)
    (champion_dir / "manifest.json").write_text("{}", encoding="utf-8")

    champion_json = tmp_path / "champion.json"
    _write_champion_json(champion_json, champion_dir)

    result = execute_rollback(
        symbol="AAPL",
        timeframe="1D",
        champion_json_path=champion_json,
        approved_root=approved,
        public_key_path=pub,
    )

    assert result.ok is True


def test_rollback_no_current_model_still_copies_champion(tmp_path: Path) -> None:
    """If no current approved model exists, champion is still copied in."""
    approved = tmp_path / "approved"
    # Do NOT create current_dir

    champion_dir = tmp_path / "champion"
    _make_model_dir(champion_dir)

    champion_json = tmp_path / "champion.json"
    _write_champion_json(champion_json, champion_dir)

    result = execute_rollback(
        symbol="AAPL",
        timeframe="1D",
        champion_json_path=champion_json,
        approved_root=approved,
    )

    assert result.ok is True
    assert result.retired_path is None  # nothing to retire
    assert (approved / "AAPL" / "1D" / "model.cbm").exists()


def test_rollback_case_insensitive_symbol_timeframe(tmp_path: Path) -> None:
    """Lowercase symbol/timeframe inputs are normalised to uppercase."""
    approved = tmp_path / "approved"
    _make_model_dir(approved / "AAPL" / "1D")

    champion_dir = tmp_path / "champion"
    _make_model_dir(champion_dir)

    champion_json = tmp_path / "champion.json"
    _write_champion_json(champion_json, champion_dir)

    result = execute_rollback(
        symbol="aapl",
        timeframe="1d",
        champion_json_path=champion_json,
        approved_root=approved,
    )

    assert result.ok is True
    assert result.symbol == "AAPL"
    assert result.timeframe == "1D"


# ── save_champion_record() tests ──────────────────────────────────────────────

def test_save_champion_record_writes_valid_json(tmp_path: Path) -> None:
    champion_dir = tmp_path / "champion_files"
    champion_dir.mkdir()
    out = tmp_path / "registry" / "champion.json"

    save_champion_record(
        symbol="AAPL",
        timeframe="1D",
        champion_model_dir=champion_dir,
        champion_json_path=out,
        artifact_id=77,
        sha256="abc123",
    )

    assert out.exists()
    data = json.loads(out.read_text())
    assert data["symbol"] == "AAPL"
    assert data["timeframe"] == "1D"
    assert data["champion_model_dir"] == str(champion_dir)
    assert data["artifact_id"] == 77
    assert data["sha256"] == "abc123"
    assert "set_at" in data


# ── drift_monitor integration tests ──────────────────────────────────────────

def test_trigger_rollback_called_on_drift_breach(tmp_path: Path) -> None:
    """_trigger_rollback is called when streak >= breach_days in evaluate_drift()."""
    from octa.core.governance.drift_monitor import evaluate_drift
    from unittest.mock import patch as _patch

    # Stub ledger with enough NAVs to exceed window
    dummy_navs = [(None, float(i)) for i in range(1, 30)]
    for i, (_, nav) in enumerate(dummy_navs):
        dummy_navs[i] = (__import__("datetime").datetime(2026, 1, i + 1, tzinfo=__import__("datetime").timezone.utc), nav)

    # Low KPI → every day is a breach day → streak will reach 3 quickly
    cfg = {"window_days": 5, "breach_days": 3, "kpi_threshold": 9999.0}

    rollback_calls = []

    with (
        _patch("octa.core.governance.drift_monitor._collect_navs", return_value=dummy_navs),
        _patch("octa.core.governance.drift_monitor._load_state", return_value={"streak": 3}),
        _patch("octa.core.governance.drift_monitor._save_state"),
        _patch("octa.core.governance.drift_monitor.evaluate_write_permission", return_value=MagicMock(blocked=False)),
        _patch("octa.core.governance.drift_monitor._write_drift_audit"),
        _patch("octa.core.governance.drift_monitor._trigger_rollback", side_effect=lambda *a, **kw: rollback_calls.append(a)) as mock_trigger,
    ):
        decision = evaluate_drift(
            ledger_dir=str(tmp_path / "ledger"),
            model_key="AAPL_1D",
            gate="production",
            timeframe="1D",
            bucket="default",
            cfg=cfg,
        )

    assert decision.disabled is True
    assert mock_trigger.called
    assert len(rollback_calls) == 1


def test_trigger_rollback_not_called_when_healthy(tmp_path: Path) -> None:
    """_trigger_rollback is NOT called when streak < breach_days."""
    from octa.core.governance.drift_monitor import evaluate_drift
    from unittest.mock import patch as _patch
    import datetime as _dt

    dummy_navs = [
        (_dt.datetime(2026, 1, i + 1, tzinfo=_dt.timezone.utc), float(i + 10))
        for i in range(30)
    ]

    # High KPI threshold that won't be reached → no breach
    cfg = {"window_days": 5, "breach_days": 3, "kpi_threshold": -999.0}

    with (
        _patch("octa.core.governance.drift_monitor._collect_navs", return_value=dummy_navs),
        _patch("octa.core.governance.drift_monitor._load_state", return_value={}),
        _patch("octa.core.governance.drift_monitor._save_state"),
        _patch("octa.core.governance.drift_monitor.evaluate_write_permission", return_value=MagicMock(blocked=False)),
        _patch("octa.core.governance.drift_monitor._trigger_rollback") as mock_trigger,
    ):
        decision = evaluate_drift(
            ledger_dir=str(tmp_path / "ledger"),
            model_key="AAPL_1D",
            gate="production",
            timeframe="1D",
            bucket="default",
            cfg=cfg,
        )

    assert decision.disabled is False
    mock_trigger.assert_not_called()


def test_trigger_rollback_best_effort_does_not_raise(tmp_path: Path) -> None:
    """A failing rollback executor must not propagate exceptions from _trigger_rollback."""
    from octa.core.governance.drift_monitor import _trigger_rollback

    # champion.json doesn't exist → execute_rollback returns ok=False silently
    _trigger_rollback("AAPL_1D", "1D", str(tmp_path / "nonexistent" / "champion.json"))
    # No exception raised
