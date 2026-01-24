from __future__ import annotations

import argparse
import os
from typing import Any, Dict

import yaml  # type: ignore[import]

from .fingerprint import sha256_hexdigest
from .secrets import (
    EncryptedFileBackend,
    EnvSecretsBackend,
    KeyringBackend,
)
from .settings import FabricSettings, Mode


class LoaderError(Exception):
    pass


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Shallow merge; b overlays a deterministically."""
    res = dict(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(res.get(k), dict):
            res[k] = merge(res[k], v)
        else:
            res[k] = v
    return res


def env_overrides(prefix: str = "OCTA_CFG_") -> Dict[str, Any]:
    res: Dict[str, Any] = {}
    for k, v in os.environ.items():
        if k.startswith(prefix):
            key = k[len(prefix) :]
            # support nested keys using __ separator
            parts = key.split("__")
            target = res
            for p in parts[:-1]:
                target = target.setdefault(p.lower(), {})
            target[parts[-1].lower()] = _coerce_env_value(v)
    return res


def _coerce_env_value(v: str) -> Any:
    # simple heuristics
    if v.lower() in ("true", "false"):
        return v.lower() == "true"
    try:
        return int(v)
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        pass
    return v


def build_settings(
    config_dir: str, mode: Mode, secrets_backend: Any | None = None
) -> FabricSettings:
    base = load_yaml(os.path.join(config_dir, "base.yaml"))
    overlay = load_yaml(os.path.join(config_dir, f"{mode.value.lower()}.yaml"))
    merged = merge(base, overlay)
    merged = merge(merged, env_overrides())
    # compute fingerprint excluding secrets
    fingerprint = sha256_hexdigest(merged)
    # attach fingerprint for verification
    merged.setdefault("signed_fingerprint", None)
    merged.setdefault("operator", {})
    merged["fingerprint"] = fingerprint
    # pydantic v2: use `model_validate` instead of deprecated `parse_obj`
    settings = FabricSettings.model_validate(merged)

    # Mode-specific checks
    if settings.mode == Mode.LIVE:
        # require operator token and signed fingerprint and verify
        op_token = os.getenv("OCTA_OPERATOR_TOKEN") or settings.operator.operator_token
        signed = settings.signed_fingerprint or os.getenv("OCTA_SIGNED_FINGERPRINT")
        if not op_token or not signed:
            raise LoaderError(
                "LIVE mode requires operator token and signed fingerprint"
            )
        # verify signature using HMAC-SHA256 with operator token as key
        import hashlib
        import hmac

        expected = hmac.new(
            op_token.encode("utf-8"), fingerprint.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, signed):
            raise LoaderError("signed fingerprint verification failed")

    return settings


def choose_secrets_backend() -> Any:
    # priority: keyring -> encrypted file -> env
    try:
        kb = KeyringBackend()
        # probe keyring availability
        try:
            _ = kb.get("probe")
            return kb
        except Exception:
            pass
    except Exception:
        pass
    # encrypted file
    path = os.getenv("OCTA_SECRETS_FILE")
    if path:
        return EncryptedFileBackend(path=path)
    # fallback to env
    return EnvSecretsBackend()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="octa_fabric.loader")
    p.add_argument("--mode", choices=[m.value for m in Mode], required=True)
    p.add_argument("--config-dir", default="configs")
    args = p.parse_args(argv)
    try:
        backend = choose_secrets_backend()
        settings = build_settings(
            args.config_dir, Mode(args.mode), secrets_backend=backend
        )
    except Exception as e:
        print("Configuration/loader error:", e)
        return 2
    # print validated summary without secrets
    d = settings.dict()
    d.pop("signed_fingerprint", None)
    print("Validated config:")
    print(f" mode: {d.get('mode')}")
    print(f" env: {d.get('env')}")
    print(f" service: {d.get('service_name')}")
    print(f" allow_trading: {d.get('allow_trading')}")
    print(f" fingerprint: {d.get('fingerprint')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
