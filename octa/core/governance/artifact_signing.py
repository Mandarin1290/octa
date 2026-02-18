"""Ed25519 artifact signing and verification.

Produces deterministic ``<artifact>.sha256`` and ``<artifact>.sig`` sidecar
files.  Keys are loaded from file paths, optionally resolved via environment
variables (``OCTA_SIGNING_KEY_PATH`` / ``OCTA_VERIFY_KEY_PATH``).

Signing format
--------------
* ``<artifact>.sha256`` — hex-encoded SHA-256 digest of the artifact.
* ``<artifact>.sig``    — base64-encoded Ed25519 signature of the raw
  artifact bytes.

Verification checks that the signature is valid for the artifact content
using the corresponding public key.
"""

from __future__ import annotations

import base64
import hashlib
import os
from pathlib import Path
from typing import Optional

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives import serialization

ENV_SIGNING_KEY_PATH = "OCTA_SIGNING_KEY_PATH"
ENV_VERIFY_KEY_PATH = "OCTA_VERIFY_KEY_PATH"


def compute_sha256(path: Path) -> str:
    """Return hex-encoded SHA-256 of file at *path*."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_raw_key(key_path: Path) -> bytes:
    """Load raw 32-byte key from file (binary or PEM)."""
    raw = key_path.read_bytes()
    # If exactly 32 bytes, treat as raw Ed25519 seed
    if len(raw) == 32:
        return raw
    # Try stripping a single trailing newline (common for echo-generated files)
    stripped = raw.rstrip(b"\n\r")
    if len(stripped) == 32:
        return stripped
    # Otherwise try PEM
    raise ValueError(
        f"Key file {key_path} has unexpected length {len(raw)}.  "
        "Expected 32-byte raw Ed25519 seed."
    )


def resolve_signing_key_path(explicit: Optional[Path] = None) -> Path:
    """Resolve signing key path from explicit arg or environment."""
    if explicit is not None:
        return Path(explicit)
    env_val = os.environ.get(ENV_SIGNING_KEY_PATH, "").strip()
    if env_val:
        return Path(env_val)
    raise RuntimeError(
        f"No signing key path provided and {ENV_SIGNING_KEY_PATH} not set."
    )


def resolve_verify_key_path(explicit: Optional[Path] = None) -> Path:
    """Resolve verification (public) key path from explicit arg or environment."""
    if explicit is not None:
        return Path(explicit)
    env_val = os.environ.get(ENV_VERIFY_KEY_PATH, "").strip()
    if env_val:
        return Path(env_val)
    raise RuntimeError(
        f"No verify key path provided and {ENV_VERIFY_KEY_PATH} not set."
    )


def generate_keypair(private_path: Path, public_path: Path) -> None:
    """Generate a new Ed25519 keypair and write raw 32-byte files."""
    priv = Ed25519PrivateKey.generate()
    priv_bytes = priv.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub_bytes = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    private_path.parent.mkdir(parents=True, exist_ok=True)
    public_path.parent.mkdir(parents=True, exist_ok=True)
    private_path.write_bytes(priv_bytes)
    public_path.write_bytes(pub_bytes)


def sign_artifact(artifact_path: Path, private_key_path: Path) -> tuple[Path, Path]:
    """Sign an artifact file producing .sha256 and .sig sidecars.

    Returns (sha256_path, sig_path).
    """
    artifact_path = Path(artifact_path)
    if not artifact_path.exists():
        raise FileNotFoundError(f"Artifact not found: {artifact_path}")

    priv_bytes = _load_raw_key(private_key_path)
    priv = Ed25519PrivateKey.from_private_bytes(priv_bytes)

    content = artifact_path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    signature = priv.sign(content)
    sig_b64 = base64.b64encode(signature).decode("ascii")

    sha_path = artifact_path.parent / (artifact_path.name + ".sha256")
    sig_path = artifact_path.parent / (artifact_path.name + ".sig")
    sha_path.write_text(f"{digest}  {artifact_path.name}\n", encoding="utf-8")
    sig_path.write_text(sig_b64 + "\n", encoding="utf-8")

    return sha_path, sig_path


def verify_artifact(
    artifact_path: Path,
    public_key_path: Path,
    *,
    sig_path: Optional[Path] = None,
    sha256_path: Optional[Path] = None,
) -> bool:
    """Verify an artifact's signature and SHA-256 digest.

    Returns True if both the SHA-256 and Ed25519 signature match.
    Returns False on any mismatch or missing sidecar.
    """
    artifact_path = Path(artifact_path)
    if not artifact_path.exists():
        return False

    if sig_path is None:
        sig_path = artifact_path.parent / (artifact_path.name + ".sig")
    if sha256_path is None:
        sha256_path = artifact_path.parent / (artifact_path.name + ".sha256")

    if not sig_path.exists() or not sha256_path.exists():
        return False

    pub_bytes = _load_raw_key(public_key_path)
    pub = Ed25519PublicKey.from_public_bytes(pub_bytes)

    content = artifact_path.read_bytes()

    # Verify SHA-256
    expected_digest = hashlib.sha256(content).hexdigest()
    sha_line = sha256_path.read_text(encoding="utf-8").strip()
    # Format: "<hex>  <filename>" or just "<hex>"
    file_digest = sha_line.split()[0] if sha_line else ""
    if file_digest != expected_digest:
        return False

    # Verify Ed25519 signature
    sig_b64 = sig_path.read_text(encoding="utf-8").strip()
    try:
        sig_bytes = base64.b64decode(sig_b64)
        pub.verify(sig_bytes, content)
        return True
    except Exception:
        return False
