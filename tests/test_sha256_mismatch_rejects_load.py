"""Test SHA-256 manifest cross-verification at model load (I2).

Verifies that load_approved_model() returns ok=False when the manifest's
sha256 field does not match the actual model file's SHA-256 digest.
"""

import hashlib
import json

import pytest

from octa.core.governance.artifact_signing import compute_sha256, generate_keypair, sign_artifact
from octa.models.approved_loader import load_approved_model


def _setup_model(tmp_path, model_content: bytes = b"fake catboost model content"):
    """Create a signed model in approved_root. Returns (approved_root, pub_key, actual_sha256)."""
    priv_key = tmp_path / "signing.key"
    pub_key = tmp_path / "verify.pub"
    generate_keypair(priv_key, pub_key)

    approved_root = tmp_path / "approved"
    model_dir = approved_root / "AAPL" / "1D"
    model_dir.mkdir(parents=True)
    model_path = model_dir / "model.cbm"
    model_path.write_bytes(model_content)

    actual_sha256 = hashlib.sha256(model_content).hexdigest()
    sign_artifact(model_path, priv_key)  # writes .sha256 and .sig sidecars

    return approved_root, model_dir, model_path, pub_key, actual_sha256, priv_key


def _write_manifest(model_dir, sha256_value=None):
    manifest = {
        "symbol": "AAPL",
        "timeframe": "1D",
        "model_file": "model.cbm",
        "promoted_at_utc": "2026-01-01T00:00:00+00:00",
    }
    if sha256_value is not None:
        manifest["sha256"] = sha256_value
    (model_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )


def test_valid_manifest_sha256_loads_successfully(tmp_path):
    """load_approved_model() succeeds when manifest sha256 matches actual file."""
    approved_root, model_dir, _, pub_key, actual_sha256, _ = _setup_model(tmp_path)
    _write_manifest(model_dir, sha256_value=actual_sha256)

    result = load_approved_model("AAPL", "1D", public_key_path=pub_key, approved_root=approved_root)
    assert result.ok, f"Expected ok=True, got reason: {result.reason}"
    assert result.reason == "approved"


def test_manifest_sha256_mismatch_rejects_load(tmp_path):
    """load_approved_model() returns ok=False when manifest sha256 is wrong."""
    approved_root, model_dir, _, pub_key, _, _ = _setup_model(tmp_path)
    wrong_sha256 = "a" * 64  # valid hex length but definitely wrong
    _write_manifest(model_dir, sha256_value=wrong_sha256)

    result = load_approved_model("AAPL", "1D", public_key_path=pub_key, approved_root=approved_root)
    assert not result.ok, "Expected ok=False when manifest sha256 mismatches"
    assert result.reason == "sha256_manifest_mismatch"


def test_manifest_without_sha256_field_skips_cross_verify(tmp_path):
    """Backwards compat: manifest without sha256 field loads successfully."""
    approved_root, model_dir, _, pub_key, _, _ = _setup_model(tmp_path)
    _write_manifest(model_dir, sha256_value=None)  # no sha256 field

    result = load_approved_model("AAPL", "1D", public_key_path=pub_key, approved_root=approved_root)
    assert result.ok, f"Expected ok=True when manifest has no sha256 field, got: {result.reason}"


def test_signature_failure_rejects_before_sha256_check(tmp_path):
    """Corrupted signature fails with signature_verification_failed, not sha256_manifest_mismatch."""
    approved_root, model_dir, _, pub_key, actual_sha256, _ = _setup_model(tmp_path)
    _write_manifest(model_dir, sha256_value=actual_sha256)

    # Corrupt the .sig sidecar
    sig_path = model_dir / "model.cbm.sig"
    sig_path.write_text("corrupted_base64_data_here\n")

    result = load_approved_model("AAPL", "1D", public_key_path=pub_key, approved_root=approved_root)
    assert not result.ok
    assert result.reason == "signature_verification_failed"


def test_tampered_model_file_caught_by_sig_verify(tmp_path):
    """Tampered model content fails via verify_artifact (sha256 sidecar mismatch)."""
    approved_root, model_dir, model_path, pub_key, _, _ = _setup_model(tmp_path)
    # Write manifest with correct original sha256
    original_sha256 = compute_sha256(model_path)
    _write_manifest(model_dir, sha256_value=original_sha256)

    # Tamper with the model file content (without updating sidecars)
    model_path.write_bytes(b"tampered model content different from original")

    result = load_approved_model("AAPL", "1D", public_key_path=pub_key, approved_root=approved_root)
    assert not result.ok
    # Could be sig failure or sha256 sidecar mismatch — both are valid rejections
    assert result.reason in ("signature_verification_failed", "sha256_manifest_mismatch")


def test_model_not_found_returns_ok_false(tmp_path):
    """Missing model file returns ok=False."""
    priv_key = tmp_path / "signing.key"
    pub_key = tmp_path / "verify.pub"
    generate_keypair(priv_key, pub_key)
    approved_root = tmp_path / "approved"

    result = load_approved_model("AAPL", "1D", public_key_path=pub_key, approved_root=approved_root)
    assert not result.ok
    assert "model_not_found" in result.reason


def test_missing_manifest_returns_ok_false(tmp_path):
    """Missing manifest.json returns ok=False."""
    approved_root, model_dir, _, pub_key, _, _ = _setup_model(tmp_path)
    # Do NOT write manifest

    result = load_approved_model("AAPL", "1D", public_key_path=pub_key, approved_root=approved_root)
    assert not result.ok
    assert "manifest_missing" in result.reason


def test_sha256_mismatch_with_correct_sig_is_caught(tmp_path):
    """Cross-verify catches a case where the signature is valid but manifest sha256 is stale.

    Scenario: model was re-signed (new .sha256 + .sig) but manifest not updated.
    verify_artifact() passes (new sig matches new content).
    Our cross-verify catches the manifest.sha256 mismatch.
    """
    approved_root, model_dir, model_path, pub_key, original_sha256, priv_key = _setup_model(tmp_path)

    # Write manifest with old (correct for original file) sha256
    _write_manifest(model_dir, sha256_value=original_sha256)

    # Replace model content and re-sign — manifest sha256 is now stale
    new_content = b"new model version content"
    model_path.write_bytes(new_content)
    sign_artifact(model_path, priv_key)  # updates .sha256 and .sig for new content

    # verify_artifact() will pass (new sig + new sha256 sidecar both match new content)
    # But manifest.sha256 still has old sha256 → cross-verify should fail
    result = load_approved_model("AAPL", "1D", public_key_path=pub_key, approved_root=approved_root)
    assert not result.ok
    assert result.reason == "sha256_manifest_mismatch"
