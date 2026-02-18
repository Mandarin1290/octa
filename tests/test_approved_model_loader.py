"""Tests for approved-only model loading and promotion."""

from __future__ import annotations

from pathlib import Path

import pytest

from octa.core.governance.artifact_signing import generate_keypair, sign_artifact
from octa.models.approved_loader import (
    ModelLoadResult,
    list_approved_models,
    load_approved_model,
)
from octa.models.ops.promote import promote_model


@pytest.fixture()
def keypair(tmp_path: Path) -> tuple[Path, Path]:
    priv = tmp_path / "keys" / "signing.key"
    pub = tmp_path / "keys" / "verify.pub"
    generate_keypair(priv, pub)
    return priv, pub


@pytest.fixture()
def candidate_model(tmp_path: Path) -> Path:
    p = tmp_path / "candidate" / "model.cbm"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"catboost model bytes 12345")
    return p


def _setup_approved(
    tmp_path: Path, priv: Path, pub: Path, candidate: Path
) -> Path:
    """Promote a model and return the approved root."""
    approved_root = tmp_path / "approved"
    promote_model(
        candidate_path=candidate,
        symbol="AAPL",
        timeframe="1D",
        signing_key_path=priv,
        approved_root=approved_root,
        run_id="test_promote_001",
    )
    return approved_root


def test_promote_creates_structure(
    keypair: tuple[Path, Path], candidate_model: Path, tmp_path: Path
) -> None:
    priv, pub = keypair
    approved = tmp_path / "approved"

    report = promote_model(
        candidate_path=candidate_model,
        symbol="AAPL",
        timeframe="1D",
        signing_key_path=priv,
        approved_root=approved,
        run_id="test_run",
    )

    assert report["status"] == "promoted"
    assert report["symbol"] == "AAPL"
    assert report["timeframe"] == "1D"
    assert Path(report["model_path"]).exists()
    assert Path(report["sig_path"]).exists()
    assert Path(report["sha256_path"]).exists()
    assert Path(report["manifest_path"]).exists()


def test_load_approved_model_ok(
    keypair: tuple[Path, Path], candidate_model: Path, tmp_path: Path
) -> None:
    priv, pub = keypair
    approved = _setup_approved(tmp_path, priv, pub, candidate_model)

    result = load_approved_model(
        "AAPL", "1D", public_key_path=pub, approved_root=approved
    )
    assert result.ok is True
    assert result.reason == "approved"
    assert result.model_path is not None
    assert result.model_path.exists()
    assert result.manifest.get("symbol") == "AAPL"


def test_load_approved_model_not_found(keypair: tuple[Path, Path], tmp_path: Path) -> None:
    _, pub = keypair
    approved = tmp_path / "empty_approved"
    result = load_approved_model(
        "AAPL", "1D", public_key_path=pub, approved_root=approved
    )
    assert result.ok is False
    assert "model_not_found" in result.reason


def test_load_approved_model_no_manifest(
    keypair: tuple[Path, Path], tmp_path: Path
) -> None:
    priv, pub = keypair
    # Create model + sig but no manifest
    model_dir = tmp_path / "approved" / "AAPL" / "1D"
    model_dir.mkdir(parents=True, exist_ok=True)
    model = model_dir / "model.cbm"
    model.write_bytes(b"raw bytes")
    sign_artifact(model, priv)

    result = load_approved_model(
        "AAPL", "1D", public_key_path=pub, approved_root=tmp_path / "approved"
    )
    assert result.ok is False
    assert "manifest" in result.reason


def test_load_approved_model_bad_signature(
    keypair: tuple[Path, Path], candidate_model: Path, tmp_path: Path
) -> None:
    priv, pub = keypair
    approved = _setup_approved(tmp_path, priv, pub, candidate_model)

    # Tamper with model file
    model_path = approved / "AAPL" / "1D" / "model.cbm"
    model_path.write_bytes(b"TAMPERED")

    result = load_approved_model(
        "AAPL", "1D", public_key_path=pub, approved_root=approved
    )
    assert result.ok is False
    assert result.reason == "signature_verification_failed"


def test_load_approved_model_missing_public_key(
    keypair: tuple[Path, Path], candidate_model: Path, tmp_path: Path
) -> None:
    priv, pub = keypair
    approved = _setup_approved(tmp_path, priv, pub, candidate_model)

    result = load_approved_model(
        "AAPL", "1D",
        public_key_path=tmp_path / "nonexistent.pub",
        approved_root=approved,
    )
    assert result.ok is False
    assert "public_key_not_found" in result.reason


def test_list_approved_models(
    keypair: tuple[Path, Path], candidate_model: Path, tmp_path: Path
) -> None:
    priv, pub = keypair
    approved = _setup_approved(tmp_path, priv, pub, candidate_model)

    models = list_approved_models(approved)
    assert len(models) == 1
    assert models[0]["symbol"] == "AAPL"
    assert models[0]["timeframe"] == "1D"
    assert models[0]["has_model"] is True
    assert models[0]["has_sig"] is True


def test_list_approved_models_empty(tmp_path: Path) -> None:
    models = list_approved_models(tmp_path / "nonexistent")
    assert models == []


def test_promote_model_candidate_not_found(
    keypair: tuple[Path, Path], tmp_path: Path
) -> None:
    priv, _ = keypair
    with pytest.raises(FileNotFoundError, match="Candidate model not found"):
        promote_model(
            candidate_path=tmp_path / "no_such.cbm",
            symbol="AAPL",
            timeframe="1D",
            signing_key_path=priv,
            approved_root=tmp_path / "approved",
        )


def test_promote_model_signing_key_not_found(
    candidate_model: Path, tmp_path: Path
) -> None:
    with pytest.raises(FileNotFoundError, match="Signing key not found"):
        promote_model(
            candidate_path=candidate_model,
            symbol="AAPL",
            timeframe="1D",
            signing_key_path=tmp_path / "no_such.key",
            approved_root=tmp_path / "approved",
        )
