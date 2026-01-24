from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SecretRef:
    key: str


# Typed keys (never print values)
TELEGRAM_BOT_TOKEN = SecretRef("TELEGRAM_BOT_TOKEN")
OCTA_MASTER_KEY = SecretRef("OCTA_MASTER_KEY")
OPENGAMMA_BEARER_TOKEN = SecretRef("OPENGAMMA_BEARER_TOKEN")
IBKR_ACCOUNT = SecretRef("IBKR_ACCOUNT")


def _get_from_env(key: str) -> Optional[str]:
    v = os.getenv(key)
    if v is None:
        return None
    v2 = v.strip()
    return v2 if v2 else None


def _get_from_keyring(key: str) -> Optional[str]:
    try:
        import keyring  # type: ignore

        return keyring.get_password("octa", key)
    except Exception:
        return None


def _get_from_vault(key: str, *, url: str, token_env: str) -> Optional[str]:
    # Optional integration (hvac). Disabled by default.
    token = _get_from_env(token_env)
    if not token:
        return None
    try:
        import hvac  # type: ignore

        client = hvac.Client(url=url, token=token)
        if not client.is_authenticated():
            return None
        # Convention: secret/data/octa/<key>
        resp = client.secrets.kv.v2.read_secret_version(path=f"octa/{key.lower()}")
        data = (resp or {}).get("data", {}).get("data", {})
        v = data.get("value")
        return str(v) if v is not None else None
    except Exception:
        return None


def get_secret(key: str, *, cfg: dict | None = None) -> Optional[str]:
    """Retrieve secret without ever printing it.

    Resolution order:
    - Vault (optional, if enabled)
    - OS keyring (optional)
    - Environment variable
    """

    cfg = cfg or {}
    sec = cfg.get("security", {}) if isinstance(cfg.get("security"), dict) else {}
    vault = sec.get("vault", {}) if isinstance(sec.get("vault"), dict) else {}

    if bool(vault.get("enabled", False)):
        v = _get_from_vault(key, url=str(vault.get("url", "")), token_env=str(vault.get("token_env", "VAULT_TOKEN")))
        if v:
            return v

    secrets_cfg = sec.get("secrets", {}) if isinstance(sec.get("secrets"), dict) else {}
    if bool(secrets_cfg.get("prefer_keyring", True)):
        v = _get_from_keyring(key)
        if v:
            return v

    return _get_from_env(key)


__all__ = [
    "SecretRef",
    "get_secret",
    "TELEGRAM_BOT_TOKEN",
    "OCTA_MASTER_KEY",
    "OPENGAMMA_BEARER_TOKEN",
    "IBKR_ACCOUNT",
]
