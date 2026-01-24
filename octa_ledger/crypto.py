from __future__ import annotations

import base64
import hashlib
from typing import List

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_event_canonical(canonical_json: str) -> str:
    return sha256_hex(canonical_json.encode("utf-8"))


def sign_batch(private_key_bytes: bytes, hashes: List[str]) -> str:
    priv = Ed25519PrivateKey.from_private_bytes(private_key_bytes)
    blob = "".join(hashes).encode("utf-8")
    sig = priv.sign(blob)
    return base64.b64encode(sig).decode()


def verify_batch_signature(
    public_key_bytes: bytes, hashes: List[str], signature_b64: str
) -> bool:
    pub = Ed25519PublicKey.from_public_bytes(public_key_bytes)
    blob = "".join(hashes).encode("utf-8")
    sig = base64.b64decode(signature_b64)
    try:
        pub.verify(sig, blob)
        return True
    except Exception:
        return False


def public_from_private(private_key_bytes: bytes) -> bytes:
    priv = Ed25519PrivateKey.from_private_bytes(private_key_bytes)
    pub = priv.public_key()
    return pub.public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
    )
