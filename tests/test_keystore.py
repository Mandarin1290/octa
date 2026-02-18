"""Tests for keystore rotation and revocation."""

from __future__ import annotations

from pathlib import Path

import pytest

from octa.core.governance.artifact_signing import sign_artifact, verify_artifact
from octa.core.governance.keystore import Keystore


@pytest.fixture()
def ks(tmp_path: Path) -> Keystore:
    return Keystore(root=tmp_path / "keys")


def test_initialize(ks: Keystore) -> None:
    result = ks.initialize()
    assert result["status"] == "initialized"
    assert ks.has_active_key() is True
    assert len(ks.active_signing_key.read_bytes()) == 32
    assert len(ks.active_verify_key.read_bytes()) == 32


def test_initialize_idempotent(ks: Keystore) -> None:
    ks.initialize()
    result = ks.initialize()
    assert result["status"] == "already_initialized"


def test_rotate_archives_old_key(ks: Keystore) -> None:
    ks.initialize()
    old_priv = ks.active_signing_key.read_bytes()
    old_pub = ks.active_verify_key.read_bytes()

    result = ks.rotate(key_id="key_v1")
    assert result["status"] == "rotated"
    assert result["archived_as"] == "key_v1"

    # New keys are different
    new_priv = ks.active_signing_key.read_bytes()
    new_pub = ks.active_verify_key.read_bytes()
    assert new_priv != old_priv
    assert new_pub != old_pub

    # Old keys archived
    prev = ks.list_previous_keys()
    assert len(prev) == 1
    assert prev[0]["key_id"] == "key_v1"
    assert prev[0]["has_private_key"] is True
    assert prev[0]["revoked"] is False


def test_revoke_key(ks: Keystore) -> None:
    ks.initialize()
    ks.rotate(key_id="key_v1")
    result = ks.revoke("key_v1")
    assert result["status"] == "revoked"
    assert ks.is_revoked("key_v1") is True
    assert ks.verify_not_revoked("key_v1") is False

    # Private key deleted
    prev = ks.list_previous_keys()
    assert prev[0]["has_private_key"] is False


def test_revoke_idempotent(ks: Keystore) -> None:
    ks.initialize()
    ks.rotate(key_id="key_v1")
    ks.revoke("key_v1")
    result = ks.revoke("key_v1")
    assert result["status"] == "already_revoked"


def test_verify_not_revoked(ks: Keystore) -> None:
    assert ks.verify_not_revoked("nonexistent") is True
    assert ks.is_revoked("nonexistent") is False


def test_rotation_preserves_old_signatures(ks: Keystore, tmp_path: Path) -> None:
    """Artifacts signed with old key can still be verified with old pub key."""
    ks.initialize()
    artifact = tmp_path / "model.cbm"
    artifact.write_bytes(b"model content 42")

    # Sign with original key
    sign_artifact(artifact, ks.active_signing_key)
    assert verify_artifact(artifact, ks.active_verify_key) is True

    # Rotate
    ks.rotate(key_id="key_v1")

    # Old signature still verifiable with archived pub key
    old_pub = ks.root / "previous_keys" / "key_v1.pub"
    assert verify_artifact(artifact, old_pub) is True

    # But NOT verifiable with new key
    assert verify_artifact(artifact, ks.active_verify_key) is False


def test_revoked_key_signatures_invalid(ks: Keystore, tmp_path: Path) -> None:
    """After revocation, we check that the key is flagged as revoked."""
    ks.initialize()
    ks.rotate(key_id="key_v1")
    ks.revoke("key_v1")

    # The revocation check should fail
    assert ks.is_revoked("key_v1") is True
    # Private key deleted, so re-signing is impossible
    old_priv = ks.root / "previous_keys" / "key_v1.key"
    assert not old_priv.exists()


def test_multiple_rotations(ks: Keystore) -> None:
    ks.initialize()
    ks.rotate(key_id="key_v1")
    ks.rotate(key_id="key_v2")
    ks.rotate(key_id="key_v3")

    prev = ks.list_previous_keys()
    assert len(prev) == 3
    ids = {p["key_id"] for p in prev}
    assert ids == {"key_v1", "key_v2", "key_v3"}


def test_empty_revocation_list(ks: Keystore) -> None:
    assert ks.is_revoked("anything") is False
    assert ks.list_previous_keys() == []
