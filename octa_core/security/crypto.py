from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class CryptoConfig:
    master_key_b64: str


def _require_cryptography():
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401
    except Exception as e:
        raise RuntimeError("cryptography_not_available") from e


def generate_master_key_b64() -> str:
    _require_cryptography()
    key = os.urandom(32)
    return base64.urlsafe_b64encode(key).decode("utf-8")


def _parse_key(master_key_b64: str) -> bytes:
    try:
        key = base64.urlsafe_b64decode(master_key_b64.encode("utf-8"))
        if len(key) != 32:
            raise ValueError("bad_key_len")
        return key
    except Exception as e:
        raise ValueError("invalid_master_key_b64") from e


def encrypt_bytes(*, data: bytes, cfg: CryptoConfig) -> bytes:
    _require_cryptography()
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore

    key = _parse_key(cfg.master_key_b64)
    aes = AESGCM(key)
    nonce = os.urandom(12)
    ct = aes.encrypt(nonce, data, None)
    return b"OCTA1" + nonce + ct


def decrypt_bytes(*, data: bytes, cfg: CryptoConfig) -> bytes:
    _require_cryptography()
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore

    if not data.startswith(b"OCTA1"):
        raise ValueError("unknown_ciphertext_format")
    nonce = data[5:17]
    ct = data[17:]
    key = _parse_key(cfg.master_key_b64)
    aes = AESGCM(key)
    return aes.decrypt(nonce, ct, None)


def file_encrypt(path: str, *, cfg: CryptoConfig, out_path: Optional[str] = None) -> str:
    p = Path(path)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(str(p))
    out = Path(out_path) if out_path else p.with_suffix(p.suffix + ".enc")
    blob = encrypt_bytes(data=p.read_bytes(), cfg=cfg)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(blob)
    return str(out)


def file_decrypt(path: str, *, cfg: CryptoConfig, out_path: Optional[str] = None) -> str:
    p = Path(path)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(str(p))
    out = Path(out_path) if out_path else Path(str(p).removesuffix(".enc"))
    pt = decrypt_bytes(data=p.read_bytes(), cfg=cfg)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(pt)
    return str(out)


__all__ = [
    "CryptoConfig",
    "generate_master_key_b64",
    "encrypt_bytes",
    "decrypt_bytes",
    "file_encrypt",
    "file_decrypt",
]
