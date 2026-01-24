from __future__ import annotations

import base64
import hashlib
import json

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sign_metadata(private_key_bytes: bytes, meta: dict) -> str:
    priv = Ed25519PrivateKey.from_private_bytes(private_key_bytes)
    blob = json.dumps(
        meta, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    sig = priv.sign(blob)
    return base64.b64encode(sig).decode()


def verify_metadata_signature(
    public_key_bytes: bytes, meta: dict, signature_b64: str
) -> bool:
    pub = Ed25519PublicKey.from_public_bytes(public_key_bytes)
    blob = json.dumps(
        meta, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    sig = base64.b64decode(signature_b64)
    try:
        pub.verify(sig, blob)
        return True
    except Exception:
        return False
