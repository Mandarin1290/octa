"""Test quarantine_artifact() governance integration (I1).

Verifies:
- quarantine_artifact() still moves files correctly (baseline)
- with registry+artifact_id: sets lifecycle_status=QUARANTINED
- with run_id: emits EVENT_GOVERNANCE_ENFORCED via GovernanceAudit
- without run_id: no governance event emitted
- missing source files handled gracefully
"""

from unittest import mock

import pytest

from octa.core.data.storage.artifact_io import quarantine_artifact
from octa_ops.autopilot.registry import ArtifactRegistry


def _make_files(root):
    root.mkdir(parents=True, exist_ok=True)
    pkl = root / "model.pkl"
    meta = root / "model.meta.json"
    sha = root / "model.sha256"
    pkl.write_bytes(b"fake pkl content")
    meta.write_text('{"schema_version": 1}')
    sha.write_text("abc123def456" * 2 + "\n")
    return str(pkl), str(meta), str(sha)


def test_quarantine_moves_files_baseline(tmp_path):
    """Basic: quarantine_artifact() moves files and writes reason."""
    from pathlib import Path

    pkl, meta, sha = _make_files(tmp_path / "src")
    result = quarantine_artifact(pkl, meta, sha, reason="integrity_check_failed")

    qdir = Path(result["quarantine_dir"])
    assert qdir.exists()
    assert (qdir / "model.pkl").exists()
    assert (qdir / "model.meta.json").exists()
    assert (qdir / "quarantine_reason.txt").exists()
    assert (qdir / "quarantine_reason.txt").read_text() == "integrity_check_failed"


def test_quarantine_updates_registry_lifecycle_status(tmp_path):
    """quarantine_artifact() with registry+artifact_id sets lifecycle_status=QUARANTINED."""
    reg = ArtifactRegistry(root=str(tmp_path / "registry"))
    run_id = "test_quarantine_reg"
    reg.record_run_start(run_id, {})
    artifact_id = reg.add_artifact(
        run_id=run_id,
        symbol="TEST",
        timeframe="1D",
        artifact_kind="model",
        path="/fake/path",
        sha256="abc123" * 4,
        schema_version=1,
    )

    pkl = tmp_path / "model.pkl"
    pkl.write_bytes(b"fake content")

    quarantine_artifact(
        str(pkl), "", "",
        reason="sha256_mismatch",
        registry=reg,
        artifact_id=artifact_id,
    )

    cur = reg._conn.cursor()
    cur.execute("SELECT lifecycle_status FROM artifacts WHERE id=?", (artifact_id,))
    row = cur.fetchone()
    assert row[0] == "QUARANTINED", f"Expected QUARANTINED, got {row[0]}"


def test_quarantine_emits_governance_event_with_run_id(tmp_path):
    """quarantine_artifact() with run_id emits EVENT_GOVERNANCE_ENFORCED."""
    pkl = tmp_path / "model.pkl"
    pkl.write_bytes(b"fake pkl content")

    with mock.patch(
        "octa.core.governance.governance_audit.GovernanceAudit"
    ) as MockGA:
        mock_instance = mock.MagicMock()
        MockGA.return_value = mock_instance

        quarantine_artifact(
            str(pkl), "", "",
            reason="sha256_mismatch",
            run_id="test_quarantine_event_001",
        )

    MockGA.assert_called_once_with(run_id="test_quarantine_event_001")
    mock_instance.emit.assert_called_once()
    call_args = mock_instance.emit.call_args
    assert call_args[0][0] == "GOVERNANCE_ENFORCED"
    payload = call_args[0][1]
    assert payload.get("action") == "artifact_quarantined"
    assert payload.get("reason") == "sha256_mismatch"
    assert "quarantine_dir" in payload


def test_quarantine_without_run_id_no_governance_event(tmp_path):
    """quarantine_artifact() without run_id does NOT emit governance event."""
    pkl = tmp_path / "model.pkl"
    pkl.write_bytes(b"fake pkl content")

    with mock.patch(
        "octa.core.governance.governance_audit.GovernanceAudit"
    ) as MockGA:
        quarantine_artifact(str(pkl), "", "", reason="test", run_id=None)

    MockGA.assert_not_called()


def test_quarantine_without_registry_no_set_lifecycle(tmp_path):
    """quarantine_artifact() without registry does NOT call set_lifecycle_status."""
    pkl = tmp_path / "model.pkl"
    pkl.write_bytes(b"fake content")

    mock_reg = mock.MagicMock()
    # registry=None means set_lifecycle_status should not be called
    quarantine_artifact(str(pkl), "", "", reason="test", registry=None, artifact_id=None)
    mock_reg.set_lifecycle_status.assert_not_called()


def test_quarantine_missing_source_files_handled_gracefully(tmp_path):
    """quarantine_artifact() with non-existent source files returns result dict with empty moved."""
    result = quarantine_artifact(
        "/nonexistent/model.pkl",
        "/nonexistent/model.meta.json",
        "/nonexistent/model.sha256",
        reason="test_missing",
        quarantine_dir=str(tmp_path / "qdir"),  # provide writable quarantine dir
    )
    assert "quarantine_dir" in result
    assert "moved" in result
    assert result["moved"] == {}


def test_quarantine_registry_and_event_together(tmp_path):
    """quarantine_artifact() with both registry and run_id does both updates."""
    reg = ArtifactRegistry(root=str(tmp_path / "registry"))
    run_id = "test_quarantine_combined"
    reg.record_run_start(run_id, {})
    artifact_id = reg.add_artifact(
        run_id=run_id,
        symbol="COMBO",
        timeframe="5M",
        artifact_kind="model",
        path="/fake",
        sha256="abc" * 8,
        schema_version=1,
    )

    pkl = tmp_path / "model.pkl"
    pkl.write_bytes(b"content")

    with mock.patch(
        "octa.core.governance.governance_audit.GovernanceAudit"
    ) as MockGA:
        mock_instance = mock.MagicMock()
        MockGA.return_value = mock_instance

        quarantine_artifact(
            str(pkl), "", "",
            reason="gate_failed",
            registry=reg,
            artifact_id=artifact_id,
            run_id=run_id,
        )

    # Registry updated
    cur = reg._conn.cursor()
    cur.execute("SELECT lifecycle_status FROM artifacts WHERE id=?", (artifact_id,))
    assert cur.fetchone()[0] == "QUARANTINED"

    # Governance event emitted
    MockGA.assert_called_once_with(run_id=run_id)
    mock_instance.emit.assert_called_once()
