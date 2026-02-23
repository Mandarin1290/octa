from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .utils import stable_sha256


@dataclass(frozen=True)
class PolicyLoadResult:
    policy: dict[str, Any]
    policy_hash: str
    valid: bool
    errors: list[str]


_REQUIRED_KEYS = {
    "mode": dict,
    "cadence": dict,
    "training_windows": dict,
    "backoff": dict,
    "safety": dict,
    "services": dict,
    "capabilities": dict,
    "live_arming": dict,
}


def _default_policy() -> dict[str, Any]:
    return {
        "mode": {"default": "shadow", "allowed": ["shadow", "paper", "live"]},
        "cadence": {
            "tick_seconds": 30,
            "eligibility_check_seconds": 300,
            "execution_check_seconds": 30,
            "training_check_seconds": 600,
        },
        "training_windows": {
            "tz": "Europe/Berlin",
            "allowed_weekdays": [0, 1, 2, 3, 4],
            "start_hour": 6,
            "end_hour": 22,
        },
        "backoff": {"seconds": [10, 30, 60, 120], "max_errors_before_degrade": 3},
        "safety": {
            "default_execution_enabled": False,
            "fail_closed_on_unknown_sensor": True,
            "require_blessed_1d_1h": True,
            "default_mode_shadow": True,
        },
        "services": {
            "dashboard": {"enabled": True},
            "alerts": {"enabled": True},
            "broker": {"enabled": False, "required_for_execution": False},
            "training": {"enabled": True},
            "execution": {"enabled": True},
        },
        "capabilities": {
            "brain": ["READ_STATE", "WRITE_STATE", "WRITE_EVIDENCE", "WRITE_BLESSED_MODEL"],
            "dashboard_service": ["DASHBOARD_START", "WRITE_EVIDENCE"],
            "alerts_service": ["ALERT_SEND", "WRITE_EVIDENCE"],
            "broker_service": ["BROKER_CONNECT", "READ_STATE", "WRITE_EVIDENCE"],
            "training_service": ["READ_STATE", "WRITE_CANDIDATE_MODEL", "WRITE_EVIDENCE"],
            "execution_service": [
                "READ_STATE",
                "ISSUE_ORDER_INTENT",
                "SEND_ORDER",
                "WRITE_EVIDENCE",
            ],
            "risk_service": ["READ_STATE", "APPROVE_ORDER", "WRITE_EVIDENCE"],
        },
        "live_arming": {"token_path": "octa/var/state/live_armed.json", "ttl_seconds": 900},
    }


def load_policy(path: Path) -> PolicyLoadResult:
    policy = _default_policy()
    errors: list[str] = []

    if path.exists():
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            if not isinstance(raw, dict):
                errors.append("policy_not_a_mapping")
            else:
                for key, val in raw.items():
                    policy[str(key)] = val
        except Exception as exc:
            errors.append(f"policy_parse_error:{type(exc).__name__}:{exc}")
    else:
        errors.append(f"policy_missing:{path}")

    for key, typ in _REQUIRED_KEYS.items():
        if key not in policy:
            errors.append(f"missing_key:{key}")
            continue
        if not isinstance(policy[key], typ):
            errors.append(f"invalid_type:{key}:{typ.__name__}")

    allowed_modes = (
        policy.get("mode", {}).get("allowed", []) if isinstance(policy.get("mode"), dict) else []
    )
    if "shadow" not in list(allowed_modes):
        errors.append("shadow_mode_must_be_allowed")

    if not isinstance(policy.get("capabilities"), dict):
        errors.append("capabilities_missing")

    return PolicyLoadResult(
        policy=policy,
        policy_hash=stable_sha256(policy),
        valid=len(errors) == 0,
        errors=sorted(errors),
    )
