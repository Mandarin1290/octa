from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from octa_core.security.crypto import CryptoConfig, file_decrypt, file_encrypt
from octa_core.security.secrets import get_secret


@dataclass(frozen=True)
class AtRestPolicy:
    enabled: bool
    encrypt_paths: List[str]
    exclude_globs: List[str]
    master_key_env: str = "OCTA_MASTER_KEY"


def _matches_any(path: str, globs: Iterable[str]) -> bool:
    for g in globs:
        if fnmatch.fnmatch(path, g):
            return True
    return False


def should_encrypt(path: str, *, policy: AtRestPolicy) -> bool:
    if not policy.enabled:
        return False
    p = str(Path(path))
    if _matches_any(p, policy.exclude_globs or []):
        return False
    for root in policy.encrypt_paths or []:
        try:
            if Path(p).resolve().as_posix().startswith(Path(root).resolve().as_posix()):
                return True
        except Exception:
            continue
    return False


def _load_policy(security_cfg: dict) -> AtRestPolicy:
    enc = security_cfg.get("encryption_at_rest", {}) if isinstance(security_cfg.get("encryption_at_rest"), dict) else {}
    pol = enc.get("policy", {}) if isinstance(enc.get("policy"), dict) else {}
    return AtRestPolicy(
        enabled=bool(enc.get("enabled", False)),
        master_key_env=str(enc.get("master_key_env", "OCTA_MASTER_KEY")),
        encrypt_paths=list(pol.get("encrypt_paths", []) or []),
        exclude_globs=list(pol.get("exclude_globs", []) or []),
    )


def encrypt_if_needed(path: str, *, cfg: dict) -> Optional[str]:
    sec = cfg.get("security", {}) if isinstance(cfg.get("security"), dict) else {}
    policy = _load_policy(sec)
    if not should_encrypt(path, policy=policy):
        return None
    mk = get_secret(policy.master_key_env, cfg=cfg) or get_secret("OCTA_MASTER_KEY", cfg=cfg)
    if not mk:
        raise RuntimeError("encryption_enabled_but_master_key_missing")
    return file_encrypt(path, cfg=CryptoConfig(master_key_b64=mk))


def decrypt_if_needed(path: str, *, cfg: dict) -> Optional[str]:
    sec = cfg.get("security", {}) if isinstance(cfg.get("security"), dict) else {}
    policy = _load_policy(sec)
    # Only decrypt when file ends with .enc and policy enabled.
    if not policy.enabled:
        return None
    if not str(path).endswith(".enc"):
        return None
    mk = get_secret(policy.master_key_env, cfg=cfg) or get_secret("OCTA_MASTER_KEY", cfg=cfg)
    if not mk:
        raise RuntimeError("encryption_enabled_but_master_key_missing")
    return file_decrypt(path, cfg=CryptoConfig(master_key_b64=mk))


__all__ = ["AtRestPolicy", "should_encrypt", "encrypt_if_needed", "decrypt_if_needed"]
