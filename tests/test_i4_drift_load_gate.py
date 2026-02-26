"""I4: Drift monitoring enforcement at model load time.

Tests that load_approved_model() rejects models when an active drift breach
exists in the registry, and permits load when the breach is cleared or
no drift state exists.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from octa.core.governance.artifact_signing import generate_keypair
from octa.core.governance.drift_monitor import is_disabled
from octa.core.governance.governance_audit import GovernanceAudit
from octa.models.approved_loader import load_approved_model
from octa.models.ops.promote import promote_model


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def keypair(tmp_path: Path) -> tuple[Path, Path]:
    priv = tmp_path / "keys" / "signing.key"
    pub = tmp_path / "keys" / "verify.pub"
    generate_keypair(priv, pub)
    return priv, pub


@pytest.fixture()
def approved_root(tmp_path: Path, keypair: tuple[Path, Path]) -> Path:
    priv, _ = keypair
    candidate = tmp_path / "candidate" / "model.cbm"
    candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate.write_bytes(b"catboost model bytes for drift test")
    root = tmp_path / "approved"
    promote_model(
        candidate_path=candidate,
        symbol="AAPL",
        timeframe="1D",
        signing_key_path=priv,
        approved_root=root,
        run_id="drift_test_run",
    )
    return root


def _write_drift_entry(drift_dir: Path, model_key: str, disabled: bool) -> None:
    drift_dir.mkdir(parents=True, exist_ok=True)
    (drift_dir / f"{model_key}.json").write_text(
        json.dumps({"disabled": disabled, "streak": 3, "reason": "drift_breach"}),
        encoding="utf-8",
    )


# ── is_disabled() helper tests ────────────────────────────────────────────────

def test_is_disabled_no_state_returns_false(tmp_path: Path) -> None:
    """No drift state file → not blocked (model not yet evaluated)."""
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir()
    assert is_disabled("AAPL_1D", drift_registry_dir=drift_dir) is False


def test_is_disabled_active_breach_returns_true(tmp_path: Path) -> None:
    """disabled=False in registry → active breach → blocked."""
    drift_dir = tmp_path / "drift"
    _write_drift_entry(drift_dir, "AAPL_1D", disabled=False)
    assert is_disabled("AAPL_1D", drift_registry_dir=drift_dir) is True


def test_is_disabled_suppressed_entry_returns_false(tmp_path: Path) -> None:
    """disabled=True in registry → admin-suppressed (healthy) → not blocked."""
    drift_dir = tmp_path / "drift"
    _write_drift_entry(drift_dir, "AAPL_1D", disabled=True)
    assert is_disabled("AAPL_1D", drift_registry_dir=drift_dir) is False


def test_is_disabled_missing_key_fail_closed(tmp_path: Path) -> None:
    """Existing state file with no 'disabled' key → fail-closed → blocked."""
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir()
    (drift_dir / "AAPL_1D.json").write_text(
        json.dumps({"streak": 5, "reason": "drift_breach"}),
        encoding="utf-8",
    )
    assert is_disabled("AAPL_1D", drift_registry_dir=drift_dir) is True


def test_is_disabled_unreadable_file_fail_closed(tmp_path: Path) -> None:
    """Corrupt/unreadable state file → fail-closed → blocked."""
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir()
    (drift_dir / "AAPL_1D.json").write_text("NOT JSON {{{", encoding="utf-8")
    assert is_disabled("AAPL_1D", drift_registry_dir=drift_dir) is True


# ── load_approved_model() drift gate tests ────────────────────────────────────

def test_drift_breach_blocks_model_load(
    tmp_path: Path, keypair: tuple[Path, Path], approved_root: Path
) -> None:
    """Active drift breach → load_approved_model returns ok=False, reason=drift_disabled."""
    _, pub = keypair
    drift_dir = tmp_path / "drift"
    _write_drift_entry(drift_dir, "AAPL_1D", disabled=False)  # active breach

    result = load_approved_model(
        "AAPL",
        "1D",
        public_key_path=pub,
        approved_root=approved_root,
        drift_registry_dir=drift_dir,
    )

    assert result.ok is False
    assert result.reason == "drift_disabled"


def test_drift_cleared_allows_model_load(
    tmp_path: Path, keypair: tuple[Path, Path], approved_root: Path
) -> None:
    """Cleared drift (disabled=True) → load_approved_model succeeds."""
    _, pub = keypair
    drift_dir = tmp_path / "drift"
    _write_drift_entry(drift_dir, "AAPL_1D", disabled=True)  # suppressed → healthy

    result = load_approved_model(
        "AAPL",
        "1D",
        public_key_path=pub,
        approved_root=approved_root,
        drift_registry_dir=drift_dir,
    )

    assert result.ok is True
    assert result.reason == "approved"


def test_no_drift_state_allows_model_load(
    tmp_path: Path, keypair: tuple[Path, Path], approved_root: Path
) -> None:
    """No drift state file at all → load succeeds (model not yet evaluated)."""
    _, pub = keypair
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir()  # empty dir, no state files

    result = load_approved_model(
        "AAPL",
        "1D",
        public_key_path=pub,
        approved_root=approved_root,
        drift_registry_dir=drift_dir,
    )

    assert result.ok is True
    assert result.reason == "approved"


def test_drift_check_skipped_when_dir_not_provided(
    tmp_path: Path, keypair: tuple[Path, Path], approved_root: Path
) -> None:
    """drift_registry_dir=None (default) → drift check skipped entirely."""
    _, pub = keypair
    # Even if the real drift dir had breaches, no check runs without drift_registry_dir
    result = load_approved_model(
        "AAPL",
        "1D",
        public_key_path=pub,
        approved_root=approved_root,
        # drift_registry_dir not passed → backwards-compatible behaviour
    )

    assert result.ok is True


def test_drift_breach_emits_governance_event(
    tmp_path: Path, keypair: tuple[Path, Path], approved_root: Path
) -> None:
    """Drift rejection must emit EVENT_MODEL_LOAD_REJECTED to the audit chain."""
    _, pub = keypair
    drift_dir = tmp_path / "drift"
    _write_drift_entry(drift_dir, "AAPL_1D", disabled=False)

    audit = GovernanceAudit(run_id="i4_drift_test", root=tmp_path / "audit")

    result = load_approved_model(
        "AAPL",
        "1D",
        public_key_path=pub,
        approved_root=approved_root,
        drift_registry_dir=drift_dir,
        audit=audit,
    )

    assert result.ok is False
    assert result.reason == "drift_disabled"

    # Verify the governance event was written to the chain
    chain_path = tmp_path / "audit" / "i4_drift_test" / "chain.jsonl"
    assert chain_path.exists()
    records = [json.loads(line) for line in chain_path.read_text().splitlines() if line.strip()]
    assert any(r["payload"]["event_type"] == "MODEL_LOAD_REJECTED" for r in records)
    rejection = next(r for r in records if r["payload"]["event_type"] == "MODEL_LOAD_REJECTED")
    assert rejection["payload"]["data"]["reason"] == "drift_disabled"
    assert rejection["payload"]["data"]["model_key"] == "AAPL_1D"


def test_drift_breach_no_audit_still_rejects(
    tmp_path: Path, keypair: tuple[Path, Path], approved_root: Path
) -> None:
    """Drift rejection works even without an audit object (audit=None)."""
    _, pub = keypair
    drift_dir = tmp_path / "drift"
    _write_drift_entry(drift_dir, "AAPL_1D", disabled=False)

    result = load_approved_model(
        "AAPL",
        "1D",
        public_key_path=pub,
        approved_root=approved_root,
        drift_registry_dir=drift_dir,
        audit=None,
    )

    assert result.ok is False
    assert result.reason == "drift_disabled"


def test_symbol_timeframe_uppercased_for_model_key(
    tmp_path: Path, keypair: tuple[Path, Path], approved_root: Path
) -> None:
    """Model key is derived as SYMBOL_TIMEFRAME (uppercase); case-insensitive input works."""
    _, pub = keypair
    drift_dir = tmp_path / "drift"
    # Write breach for uppercase key; loader receives lowercase symbol/timeframe
    _write_drift_entry(drift_dir, "AAPL_1D", disabled=False)

    result = load_approved_model(
        "aapl",    # lowercase
        "1d",      # lowercase
        public_key_path=pub,
        approved_root=approved_root,
        drift_registry_dir=drift_dir,
    )

    assert result.ok is False
    assert result.reason == "drift_disabled"
