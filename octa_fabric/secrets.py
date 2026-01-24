from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from typing import Optional

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except Exception:  # pragma: no cover - cryptography missing
    AESGCM = None  # type: ignore

try:
    import keyring
except Exception:  # pragma: no cover - keyring optional
    keyring = None  # type: ignore


class SecretsError(Exception):
    pass


class SecretsBackendProtocol:
    def get(self, key: str) -> Optional[str]:
        raise NotImplementedError


@dataclass
class EnvSecretsBackend(SecretsBackendProtocol):
    prefix: str = "OCTA_SECRET_"

    def get(self, key: str) -> Optional[str]:
        return os.getenv(f"{self.prefix}{key}")


@dataclass
class EncryptedFileBackend(SecretsBackendProtocol):
    path: str

    def _load_key(self) -> bytes:
        raw = os.getenv("OCTA_SECRETS_KEY")
        if not raw:
            raise SecretsError("missing OCTA_SECRETS_KEY for EncryptedFileBackend")
        # accept base64 or hex
        try:
            return base64.b64decode(raw)
        except Exception:
            try:
                return bytes.fromhex(raw)
            except Exception:
                raise SecretsError(
                    "OCTA_SECRETS_KEY must be base64 or hex encoded key"
                ) from None

    def _decrypt(self, data: bytes, key: bytes) -> bytes:
        if AESGCM is None:
            raise SecretsError("cryptography not available")
        if len(key) not in (16, 24, 32):
            raise SecretsError("invalid AES key length")
        aesgcm = AESGCM(key)
        # file format: nonce(12) + ciphertext
        if len(data) < 13:
            raise SecretsError("encrypted secrets file corrupted")
        nonce = data[:12]
        ct = data[12:]
        return aesgcm.decrypt(nonce, ct, None)

    def get(self, key: str) -> Optional[str]:
        if not os.path.exists(self.path):
            raise SecretsError("secrets file not found")
        key_bytes = self._load_key()
        raw = open(self.path, "rb").read()
        plain = self._decrypt(raw, key_bytes)
        # assume simple newline-separated key=value
        for line in plain.decode("utf-8").splitlines():
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() == key:
                return v.strip()
        return None


@dataclass
class KeyringBackend(SecretsBackendProtocol):
    service_name: str = "octa-fabric"

    def get(self, key: str) -> Optional[str]:
        if keyring is None:
            raise SecretsError(
                "keyring not available; falling back to EnvSecretsBackend"
            )
        return keyring.get_password(self.service_name, key)
