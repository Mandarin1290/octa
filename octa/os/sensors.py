from __future__ import annotations

import socket
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from zoneinfo import ZoneInfo

from .eligibility import EligibilitySnapshot, read_blessed_eligibility
from .utils import load_json


@dataclass(frozen=True)
class SensorSnapshot:
    broker_ready: bool
    data_ready: bool
    risk_ok: bool
    eligibility: EligibilitySnapshot
    execution_mode_allowed: bool
    live_armed: bool
    training_window_open: bool
    unknown_flags: list[str]


def _safe_socket_open(host: str, port: int, timeout_s: float = 0.4) -> bool:
    sock = socket.socket()
    sock.settimeout(timeout_s)
    try:
        sock.connect((host, int(port)))
        return True
    except Exception:
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass


def _training_window_open(policy: dict[str, Any]) -> bool:
    windows = (
        policy.get("training_windows") if isinstance(policy.get("training_windows"), dict) else {}
    )
    tz_name = str(windows.get("tz", "Europe/Berlin"))
    now_local = datetime.now(timezone.utc).astimezone(ZoneInfo(tz_name))
    weekdays = windows.get("allowed_weekdays", [0, 1, 2, 3, 4])
    start_hour = int(windows.get("start_hour", 0))
    end_hour = int(windows.get("end_hour", 23))
    if int(now_local.weekday()) not in [int(x) for x in weekdays]:
        return False
    return start_hour <= int(now_local.hour) <= end_hour


def _live_armed(policy: dict[str, Any], now_utc: datetime) -> bool:
    lcfg = policy.get("live_arming") if isinstance(policy.get("live_arming"), dict) else {}
    token_path = Path(str(lcfg.get("token_path", "octa/var/state/live_armed.json")))
    token = load_json(token_path, default={})
    if not isinstance(token, dict):
        return False
    expires = str(token.get("expires_at_utc", "")).strip()
    if not expires:
        return False
    try:
        exp = datetime.fromisoformat(expires.replace("Z", "+00:00"))
    except Exception:
        return False
    return exp > now_utc


def collect_sensors(
    *,
    policy: dict[str, Any],
    mode: str,
    policy_valid: bool,
    blessed_registry: Path,
) -> SensorSnapshot:
    unknown_flags: list[str] = []

    broker_cfg = (
        policy.get("services", {}).get("broker", {})
        if isinstance(policy.get("services"), dict)
        else {}
    )
    broker_enabled = bool(broker_cfg.get("enabled", False))
    broker_required = bool(broker_cfg.get("required_for_execution", False))
    broker_host = str(broker_cfg.get("host", "127.0.0.1"))
    broker_port = int(broker_cfg.get("port", 7497))

    if broker_enabled:
        broker_ready = _safe_socket_open(broker_host, broker_port)
    else:
        broker_ready = not broker_required

    if broker_enabled and not broker_ready:
        unknown_flags.append("broker_not_ready")

    # Offline-safe default: if no explicit local readiness file, treat as ready.
    data_cfg = (
        policy.get("services", {}).get("data", {})
        if isinstance(policy.get("services"), dict)
        else {}
    )
    data_file = str(data_cfg.get("readiness_file", "")).strip()
    if data_file:
        data_state = load_json(Path(data_file), default={})
        data_ready = bool(isinstance(data_state, dict) and data_state.get("ready", False))
    else:
        data_ready = True

    if not data_ready:
        unknown_flags.append("data_not_ready")

    risk_file = (
        policy.get("safety", {}).get("risk_state_file")
        if isinstance(policy.get("safety"), dict)
        else None
    )
    if risk_file:
        risk_state = load_json(Path(str(risk_file)), default={})
        risk_ok = bool(isinstance(risk_state, dict) and risk_state.get("risk_ok", False))
    else:
        risk_ok = True

    if not risk_ok:
        unknown_flags.append("risk_not_ok")

    eligibility = read_blessed_eligibility(blessed_registry)

    allowed_modes = (
        policy.get("mode", {}).get("allowed", []) if isinstance(policy.get("mode"), dict) else []
    )
    execution_mode_allowed = bool(policy_valid and mode in [str(x) for x in allowed_modes])
    if not execution_mode_allowed:
        unknown_flags.append("mode_not_allowed")

    live_armed = _live_armed(policy, datetime.now(timezone.utc)) if mode == "live" else False
    if mode == "live" and not live_armed:
        unknown_flags.append("live_not_armed")

    return SensorSnapshot(
        broker_ready=broker_ready,
        data_ready=data_ready,
        risk_ok=risk_ok,
        eligibility=eligibility,
        execution_mode_allowed=execution_mode_allowed,
        live_armed=live_armed,
        training_window_open=_training_window_open(policy),
        unknown_flags=sorted(set(unknown_flags)),
    )
