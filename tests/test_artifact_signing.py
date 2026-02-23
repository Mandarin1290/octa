"""Tests for Ed25519 artifact signing and verification."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from octa.core.governance.artifact_signing import (
    ENV_SIGNING_KEY_PATH,
    ENV_VERIFY_KEY_PATH,
    compute_sha256,
    generate_keypair,
    resolve_signing_key_path,
    resolve_verify_key_path,
    sign_artifact,
    verify_artifact,
)


@pytest.fixture()
def keypair(tmp_path: Path) -> tuple[Path, Path]:
    priv = tmp_path / "keys" / "signing.key"
    pub = tmp_path / "keys" / "verify.pub"
    generate_keypair(priv, pub)
    return priv, pub


@pytest.fixture()
def artifact(tmp_path: Path) -> Path:
    p = tmp_path / "model.cbm"
    p.write_bytes(b"deterministic model bytes 42")
    return p


def test_generate_keypair(keypair: tuple[Path, Path]) -> None:
    priv, pub = keypair
    assert priv.exists()
    assert pub.exists()
    assert len(priv.read_bytes()) == 32
    assert len(pub.read_bytes()) == 32


def test_sign_and_verify(keypair: tuple[Path, Path], artifact: Path) -> None:
    priv, pub = keypair
    sha_path, sig_path = sign_artifact(artifact, priv)

    assert sha_path.exists()
    assert sig_path.exists()
    assert sha_path.name == "model.cbm.sha256"
    assert sig_path.name == "model.cbm.sig"

    # SHA256 sidecar has correct format
    sha_text = sha_path.read_text(encoding="utf-8").strip()
    assert sha_text.startswith(compute_sha256(artifact))
    assert "model.cbm" in sha_text

    # Verification passes
    assert verify_artifact(artifact, pub) is True


def test_verify_fails_on_tampered_content(keypair: tuple[Path, Path], artifact: Path) -> None:
    priv, pub = keypair
    sign_artifact(artifact, priv)

    # Tamper with artifact
    artifact.write_bytes(b"TAMPERED content")
    assert verify_artifact(artifact, pub) is False


def test_verify_fails_on_tampered_signature(keypair: tuple[Path, Path], artifact: Path) -> None:
    priv, pub = keypair
    _, sig_path = sign_artifact(artifact, priv)

    # Tamper with signature
    sig_path.write_text("AAAA" + sig_path.read_text(encoding="utf-8")[4:], encoding="utf-8")
    assert verify_artifact(artifact, pub) is False


def test_verify_fails_on_wrong_key(keypair: tuple[Path, Path], artifact: Path, tmp_path: Path) -> None:
    priv, _ = keypair
    sign_artifact(artifact, priv)

    # Generate a different keypair
    other_priv = tmp_path / "other" / "signing.key"
    other_pub = tmp_path / "other" / "verify.pub"
    generate_keypair(other_priv, other_pub)

    assert verify_artifact(artifact, other_pub) is False


def test_verify_fails_missing_sidecar(keypair: tuple[Path, Path], artifact: Path) -> None:
    _, pub = keypair
    # No sidecars exist
    assert verify_artifact(artifact, pub) is False


def test_verify_fails_missing_artifact(keypair: tuple[Path, Path], tmp_path: Path) -> None:
    _, pub = keypair
    assert verify_artifact(tmp_path / "nonexistent.bin", pub) is False


def test_resolve_signing_key_path_explicit(tmp_path: Path) -> None:
    p = tmp_path / "my.key"
    assert resolve_signing_key_path(p) == p


def test_resolve_signing_key_path_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    p = tmp_path / "env.key"
    monkeypatch.setenv(ENV_SIGNING_KEY_PATH, str(p))
    assert resolve_signing_key_path() == p


def test_resolve_signing_key_path_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ENV_SIGNING_KEY_PATH, raising=False)
    with pytest.raises(RuntimeError, match="No signing key path"):
        resolve_signing_key_path()


def test_resolve_verify_key_path_explicit(tmp_path: Path) -> None:
    p = tmp_path / "my.pub"
    assert resolve_verify_key_path(p) == p


def test_resolve_verify_key_path_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    p = tmp_path / "env.pub"
    monkeypatch.setenv(ENV_VERIFY_KEY_PATH, str(p))
    assert resolve_verify_key_path() == p


def test_sign_artifact_not_found(keypair: tuple[Path, Path], tmp_path: Path) -> None:
    priv, _ = keypair
    with pytest.raises(FileNotFoundError):
        sign_artifact(tmp_path / "no_such_file.bin", priv)


def test_compute_sha256_deterministic(artifact: Path) -> None:
    d1 = compute_sha256(artifact)
    d2 = compute_sha256(artifact)
    assert d1 == d2
    assert len(d1) == 64
