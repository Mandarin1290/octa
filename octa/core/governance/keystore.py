"""Ed25519 keystore with rotation and revocation.

Directory layout under ``octa/var/keys/``::

    active_signing_key         # raw 32-byte Ed25519 private key
    active_verify_key          # raw 32-byte Ed25519 public key
    previous_keys/
        <key_id>.key           # archived private key
        <key_id>.pub           # archived public key
    revocation_list.json       # list of revoked key IDs

Key rotation:
  1. Generate new keypair.
  2. Move current active to ``previous_keys/<key_id>``.
  3. Install new keypair as active.
  4. Emit KEY_ROTATED governance event.

Revocation:
  1. Add key ID to ``revocation_list.json``.
  2. Delete the private key (keep pub for verification audit).
  3. Emit KEY_REVOKED governance event.

Verification must check that the signing key is not revoked.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .artifact_signing import generate_keypair, verify_artifact

_DEFAULT_KEYSTORE = Path("octa") / "var" / "keys"


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Keystore:
    """Ed25519 keystore with rotation and revocation."""

    def __init__(self, root: Path = _DEFAULT_KEYSTORE) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)
        self._prev_dir = self._root / "previous_keys"
        self._prev_dir.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._root

    @property
    def active_signing_key(self) -> Path:
        return self._root / "active_signing_key"

    @property
    def active_verify_key(self) -> Path:
        return self._root / "active_verify_key"

    @property
    def revocation_list_path(self) -> Path:
        return self._root / "revocation_list.json"

    def has_active_key(self) -> bool:
        return self.active_signing_key.exists() and self.active_verify_key.exists()

    def initialize(self) -> Dict[str, str]:
        """Generate initial keypair if none exists.

        Returns dict with key paths.
        """
        if self.has_active_key():
            return {
                "status": "already_initialized",
                "signing_key": str(self.active_signing_key),
                "verify_key": str(self.active_verify_key),
            }
        generate_keypair(self.active_signing_key, self.active_verify_key)
        return {
            "status": "initialized",
            "signing_key": str(self.active_signing_key),
            "verify_key": str(self.active_verify_key),
        }

    def rotate(self, *, key_id: Optional[str] = None) -> Dict[str, Any]:
        """Rotate the active signing key.

        1. Archive current active key under previous_keys/<key_id>.
        2. Generate new keypair.
        """
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        kid = key_id or f"key_{ts}"

        # Archive current if exists
        if self.has_active_key():
            arch_priv = self._prev_dir / f"{kid}.key"
            arch_pub = self._prev_dir / f"{kid}.pub"
            arch_priv.write_bytes(self.active_signing_key.read_bytes())
            arch_pub.write_bytes(self.active_verify_key.read_bytes())

        # Generate new
        generate_keypair(self.active_signing_key, self.active_verify_key)

        return {
            "status": "rotated",
            "archived_as": kid,
            "timestamp_utc": _utc_iso(),
        }

    def revoke(self, key_id: str) -> Dict[str, Any]:
        """Revoke a key by ID.

        Adds to revocation list and deletes the private key.
        """
        revocation_list = self._load_revocation_list()
        if key_id in {r["key_id"] for r in revocation_list}:
            return {"status": "already_revoked", "key_id": key_id}

        revocation_list.append({
            "key_id": key_id,
            "revoked_at_utc": _utc_iso(),
        })
        self._save_revocation_list(revocation_list)

        # Delete private key if it exists
        priv_path = self._prev_dir / f"{key_id}.key"
        if priv_path.exists():
            priv_path.unlink()

        return {
            "status": "revoked",
            "key_id": key_id,
            "timestamp_utc": _utc_iso(),
        }

    def is_revoked(self, key_id: str) -> bool:
        """Check if a key ID is on the revocation list."""
        revocation_list = self._load_revocation_list()
        return key_id in {r["key_id"] for r in revocation_list}

    def verify_not_revoked(self, key_id: str) -> bool:
        """Return True if key is NOT revoked (valid for use)."""
        return not self.is_revoked(key_id)

    def list_previous_keys(self) -> List[Dict[str, Any]]:
        """List all archived keys."""
        keys = []
        for pub in sorted(self._prev_dir.glob("*.pub")):
            kid = pub.stem
            has_priv = (self._prev_dir / f"{kid}.key").exists()
            keys.append({
                "key_id": kid,
                "has_private_key": has_priv,
                "revoked": self.is_revoked(kid),
            })
        return keys

    def _load_revocation_list(self) -> List[Dict[str, Any]]:
        if not self.revocation_list_path.exists():
            return []
        try:
            data = json.loads(self.revocation_list_path.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            return []

    def _save_revocation_list(self, revocation_list: List[Dict[str, Any]]) -> None:
        self.revocation_list_path.write_text(
            json.dumps(revocation_list, indent=2, sort_keys=True),
            encoding="utf-8",
        )
