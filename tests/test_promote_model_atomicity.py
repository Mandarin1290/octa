"""Test atomic promotion lifecycle tracking for I1.

Verifies that promote_model() correctly tracks PENDING_PROMOTION → PAPER
on success, and PENDING_PROMOTION → PROMOTION_FAILED on copy error.
"""

from unittest import mock

import pytest

from octa.core.governance.artifact_signing import generate_keypair
from octa.models.ops.promote import promote_model
from octa_ops.autopilot.registry import ArtifactRegistry


def _setup(tmp_path, symbol="AAPL", timeframe="1D"):
    """Create registry, keypair, candidate model, and artifact record."""
    reg = ArtifactRegistry(root=str(tmp_path / "registry"))
    priv_key = tmp_path / "signing.key"
    pub_key = tmp_path / "verify.pub"
    generate_keypair(priv_key, pub_key)

    candidate = tmp_path / "candidate.cbm"
    candidate.write_bytes(b"fake catboost model content for atomicity testing")

    run_id = f"test_atomicity_{symbol}_{timeframe}"
    reg.record_run_start(run_id, {"cfg": "test"})
    artifact_id = reg.add_artifact(
        run_id=run_id,
        symbol=symbol,
        timeframe=timeframe,
        artifact_kind="model",
        path=str(candidate),
        sha256="placeholder_sha256",
        schema_version=1,
    )
    approved_root = tmp_path / "approved"
    return reg, priv_key, candidate, run_id, artifact_id, approved_root


def _get_lifecycle(reg, artifact_id):
    cur = reg._conn.cursor()
    cur.execute("SELECT lifecycle_status FROM artifacts WHERE id=?", (artifact_id,))
    return cur.fetchone()[0]


def test_successful_promotion_sets_paper_status(tmp_path):
    """Successful promote_model() sets lifecycle_status=PAPER."""
    reg, priv_key, candidate, run_id, artifact_id, approved_root = _setup(tmp_path)

    report = promote_model(
        candidate_path=candidate,
        symbol="AAPL",
        timeframe="1D",
        signing_key_path=priv_key,
        approved_root=approved_root,
        run_id=run_id,
        registry=reg,
        artifact_id=artifact_id,
    )

    assert report["status"] == "promoted"
    assert _get_lifecycle(reg, artifact_id) == "PAPER"


def test_failed_promotion_sets_promotion_failed_status(tmp_path):
    """Crash during file copy sets lifecycle_status=PROMOTION_FAILED."""
    reg, priv_key, candidate, run_id, artifact_id, approved_root = _setup(
        tmp_path, symbol="MSFT", timeframe="1H"
    )

    with mock.patch("shutil.copy2", side_effect=OSError("disk full")):
        with pytest.raises(OSError, match="disk full"):
            promote_model(
                candidate_path=candidate,
                symbol="MSFT",
                timeframe="1H",
                signing_key_path=priv_key,
                approved_root=approved_root,
                run_id=run_id,
                registry=reg,
                artifact_id=artifact_id,
            )

    assert _get_lifecycle(reg, artifact_id) == "PROMOTION_FAILED"


def test_promote_without_registry_still_works(tmp_path):
    """promote_model() without registry/artifact_id is backwards-compatible."""
    priv_key = tmp_path / "signing.key"
    pub_key = tmp_path / "verify.pub"
    generate_keypair(priv_key, pub_key)

    candidate = tmp_path / "model.cbm"
    candidate.write_bytes(b"backwards compat model content")
    approved_root = tmp_path / "approved"

    report = promote_model(
        candidate_path=candidate,
        symbol="TSLA",
        timeframe="5M",
        signing_key_path=priv_key,
        approved_root=approved_root,
    )
    assert report["status"] == "promoted"
    assert (approved_root / "TSLA" / "5M" / "model.cbm").exists()


def test_promotion_writes_manifest_with_sha256(tmp_path):
    """Promoted model manifest contains sha256 field for cross-verify."""
    import json

    reg, priv_key, candidate, run_id, artifact_id, approved_root = _setup(
        tmp_path, symbol="GOOG", timeframe="30M"
    )

    report = promote_model(
        candidate_path=candidate,
        symbol="GOOG",
        timeframe="30M",
        signing_key_path=priv_key,
        approved_root=approved_root,
        run_id=run_id,
        registry=reg,
        artifact_id=artifact_id,
    )

    manifest_path = approved_root / "GOOG" / "30M" / "manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text())
    assert "sha256" in manifest
    assert len(manifest["sha256"]) == 64  # hex SHA-256
    assert manifest["sha256"] == report["sha256"]


def test_lifecycle_is_research_before_promotion(tmp_path):
    """Artifact lifecycle_status starts as RESEARCH (registry default)."""
    reg, _, _, _, artifact_id, _ = _setup(tmp_path, symbol="IBM", timeframe="1M")
    assert _get_lifecycle(reg, artifact_id) == "RESEARCH"
