from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import yaml


class PreExecutionError(RuntimeError):
    def __init__(self, reason: str, detail: Optional[str] = None) -> None:
        msg = str(reason) if detail is None else f"{reason}:{detail}"
        super().__init__(msg)
        self.reason = str(reason)
        self.detail = detail


@dataclass(frozen=True)
class PortCheckConfig:
    host: str
    port: int
    timeout_sec: float
    retries: int
    backoff_sec: float


@dataclass(frozen=True)
class HandshakeConfig:
    enabled: bool
    mode: str
    connect_timeout_sec: float
    wait_for: tuple[str, ...]
    overall_timeout_sec: float


@dataclass(frozen=True)
class TelegramConfig:
    enabled: bool
    on_ready_message: str
    on_fail_message: str


@dataclass(frozen=True)
class PreExecutionSettings:
    enabled: bool
    tws_e2e_script: str
    tws_e2e_env_passthrough: bool
    tws_e2e_timeout_sec: int
    port_check: PortCheckConfig
    handshake: HandshakeConfig
    telegram: TelegramConfig
    ibkr_client_id: int


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str), encoding="utf-8")


def load_pre_execution_settings(config_path: Path, *, mode: str) -> PreExecutionSettings:
    raw: Dict[str, Any] = {}
    if config_path.exists():
        loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        raw = loaded if isinstance(loaded, dict) else {}
    ibkr = raw.get("ibkr", {}) if isinstance(raw.get("ibkr"), dict) else {}
    pre = raw.get("pre_execution", {}) if isinstance(raw.get("pre_execution"), dict) else {}
    port = pre.get("port_check", {}) if isinstance(pre.get("port_check"), dict) else {}
    hs = pre.get("handshake", {}) if isinstance(pre.get("handshake"), dict) else {}
    tg = pre.get("telegram", {}) if isinstance(pre.get("telegram"), dict) else {}

    wait_for_raw = hs.get("wait_for", ["nextValidId", "currentTime"])
    if isinstance(wait_for_raw, list):
        wait_for = tuple(str(x) for x in wait_for_raw)
    else:
        wait_for = ("nextValidId", "currentTime")

    settings = PreExecutionSettings(
        enabled=bool(pre.get("enabled", False)),
        tws_e2e_script=str(pre.get("tws_e2e_script", "scripts/tws_e2e.sh")),
        tws_e2e_env_passthrough=bool(pre.get("tws_e2e_env_passthrough", True)),
        tws_e2e_timeout_sec=int(pre.get("tws_e2e_timeout_sec", 600)),
        port_check=PortCheckConfig(
            host=str(port.get("host", ibkr.get("host", "127.0.0.1"))),
            port=int(port.get("port", ibkr.get("port", 7497))),
            timeout_sec=float(port.get("timeout_sec", 2)),
            retries=int(port.get("retries", 60)),
            backoff_sec=float(port.get("backoff_sec", 1)),
        ),
        handshake=HandshakeConfig(
            enabled=bool(hs.get("enabled", True)),
            mode=str(hs.get("mode", "read_only")),
            connect_timeout_sec=float(hs.get("connect_timeout_sec", 5)),
            wait_for=wait_for,
            overall_timeout_sec=float(hs.get("overall_timeout_sec", 15)),
        ),
        telegram=TelegramConfig(
            enabled=bool(tg.get("enabled", True)),
            on_ready_message=str(tg.get("on_ready_message", "TWS bereit ✅")),
            on_fail_message=str(tg.get("on_fail_message", "TWS Pre-Execution FAIL ❌: {reason}")),
        ),
        ibkr_client_id=int(ibkr.get("client_id", ibkr.get("clientId", 901))),
    )
    if not settings.enabled:
        return settings

    if settings.tws_e2e_timeout_sec <= 0:
        raise PreExecutionError("pre_execution_config_invalid", "invalid_tws_e2e_timeout")
    if settings.port_check.port <= 0:
        raise PreExecutionError("pre_execution_config_invalid", "invalid_port")
    if settings.port_check.retries <= 0:
        raise PreExecutionError("pre_execution_config_invalid", "invalid_retries")
    if settings.handshake.mode != "read_only":
        raise PreExecutionError("pre_execution_config_invalid", "handshake_mode_must_be_read_only")
    if str(mode).strip().lower() not in {"shadow", "dry-run", "paper", "live"}:
        raise PreExecutionError("pre_execution_config_invalid", "invalid_mode")
    return settings


def run_tws_e2e(
    *,
    script_path: str,
    timeout_sec: int,
    env_passthrough: bool,
    evidence_dir: Path,
    run_cmd: Callable[..., Any] = subprocess.run,
) -> Dict[str, Any]:
    evidence_dir.mkdir(parents=True, exist_ok=True)
    script = Path(script_path)
    if not script.exists():
        raise PreExecutionError("pre_execution_config_invalid", f"missing_script:{script}")
    env = dict(os.environ) if env_passthrough else {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "")}
    cp = run_cmd(
        ["bash", str(script)],
        capture_output=True,
        text=True,
        timeout=int(timeout_sec),
        env=env,
        check=False,
    )
    (evidence_dir / "tws_e2e.stdout.log").write_text(str(cp.stdout or ""), encoding="utf-8")
    (evidence_dir / "tws_e2e.stderr.log").write_text(str(cp.stderr or ""), encoding="utf-8")
    out = {"returncode": int(cp.returncode), "script": str(script), "timeout_sec": int(timeout_sec)}
    _write_json(evidence_dir / "tws_e2e.result.json", out)
    if int(cp.returncode) != 0:
        raise PreExecutionError("pre_execution_tws_e2e_failed", f"returncode={cp.returncode}")
    return out


def check_port_open(
    *,
    cfg: PortCheckConfig,
    evidence_dir: Path,
    connect_fn: Callable[..., Any] = socket.create_connection,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Dict[str, Any]:
    attempts: list[Dict[str, Any]] = []
    for idx in range(1, int(cfg.retries) + 1):
        ok = False
        err = None
        try:
            with connect_fn((cfg.host, int(cfg.port)), timeout=float(cfg.timeout_sec)):
                ok = True
        except Exception as exc:
            ok = False
            err = f"{type(exc).__name__}:{exc}"
        attempts.append({"attempt": idx, "ok": bool(ok), "error": err})
        if ok:
            result = {"host": cfg.host, "port": int(cfg.port), "attempts": attempts, "ready": True}
            _write_json(evidence_dir / "port_check.json", result)
            return result
        if idx < int(cfg.retries):
            sleep_fn(float(cfg.backoff_sec))
    result = {"host": cfg.host, "port": int(cfg.port), "attempts": attempts, "ready": False}
    _write_json(evidence_dir / "port_check.json", result)
    raise PreExecutionError("pre_execution_port_unreachable", f"{cfg.host}:{cfg.port}")


class IBInsyncReadOnlyBackend:
    def __init__(self, *, host: str, port: int, client_id: int, connect_timeout_sec: float) -> None:
        try:
            from ib_insync import IB  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise PreExecutionError("pre_execution_handshake_failed", f"ib_insync_missing:{exc}") from exc
        self._ib = IB()
        self._host = str(host)
        self._port = int(port)
        self._client_id = int(client_id)
        self._connect_timeout_sec = float(connect_timeout_sec)
        self.forbidden_calls: list[str] = []

    def connect(self) -> bool:
        ok = self._ib.connect(self._host, self._port, clientId=self._client_id, timeout=self._connect_timeout_sec)
        return bool(ok and self._ib.isConnected())

    def wait_for_next_valid_id(self, timeout_sec: float) -> bool:
        deadline = time.time() + float(timeout_sec)
        while time.time() < deadline:
            if bool(self._ib.isConnected() and getattr(self._ib.client, "isReady", lambda: False)()):
                return True
            time.sleep(0.05)
        return False

    def req_current_time(self, timeout_sec: float) -> bool:
        try:
            req_async = getattr(self._ib, "reqCurrentTimeAsync", None)
            if callable(req_async):
                val = asyncio.run(asyncio.wait_for(req_async(), timeout=float(timeout_sec)))
                return val is not None
        except Exception:
            pass
        try:
            val = self._ib.reqCurrentTime()
            return val is not None
        except Exception:
            return False

    def disconnect(self) -> None:
        try:
            if self._ib.isConnected():
                self._ib.disconnect()
        except Exception:
            pass

    # Forbidden order-related calls are explicitly modeled for guard checking.
    def placeOrder(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        self.forbidden_calls.append("placeOrder")
        raise RuntimeError("forbidden")

    def reqOpenOrders(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        self.forbidden_calls.append("reqOpenOrders")
        raise RuntimeError("forbidden")

    def reqExecutions(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        self.forbidden_calls.append("reqExecutions")
        raise RuntimeError("forbidden")


def broker_handshake_read_only(
    *,
    cfg: HandshakeConfig,
    host: str,
    port: int,
    client_id: int,
    evidence_dir: Path,
    backend_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    if not cfg.enabled:
        out = {"enabled": False, "mode": cfg.mode, "ready": False, "reason": "handshake_disabled"}
        _write_json(evidence_dir / "handshake.json", out)
        return out
    if cfg.mode != "read_only":
        raise PreExecutionError("pre_execution_handshake_not_read_only", f"mode={cfg.mode}")

    factory = backend_factory or (
        lambda **kw: IBInsyncReadOnlyBackend(
            host=kw["host"],
            port=kw["port"],
            client_id=kw["client_id"],
            connect_timeout_sec=kw["connect_timeout_sec"],
        )
    )
    backend = factory(
        host=str(host),
        port=int(port),
        client_id=int(client_id),
        connect_timeout_sec=float(cfg.connect_timeout_sec),
    )
    seen_events: list[str] = []
    try:
        if not bool(backend.connect()):
            raise PreExecutionError("pre_execution_handshake_failed", "connect_failed")

        if "nextValidId" in cfg.wait_for:
            if bool(backend.wait_for_next_valid_id(float(cfg.overall_timeout_sec))):
                seen_events.append("nextValidId")

        if "currentTime" in cfg.wait_for and not seen_events:
            if bool(backend.req_current_time(float(cfg.overall_timeout_sec))):
                seen_events.append("currentTime")

        if not seen_events:
            raise PreExecutionError("pre_execution_handshake_failed", "no_expected_callback")

        forbidden_calls = list(getattr(backend, "forbidden_calls", []) or [])
        if forbidden_calls:
            raise PreExecutionError("pre_execution_handshake_not_read_only", ",".join(forbidden_calls))

        out = {
            "enabled": True,
            "mode": cfg.mode,
            "ready": True,
            "seen_events": seen_events,
            "host": str(host),
            "port": int(port),
            "client_id": int(client_id),
        }
        _write_json(evidence_dir / "handshake.json", out)
        return out
    finally:
        try:
            backend.disconnect()
        except Exception:
            pass


def _notify(
    *,
    notifier: Any,
    telegram_cfg: TelegramConfig,
    event_type: str,
    message: str,
    reason: Optional[str] = None,
) -> bool:
    if not telegram_cfg.enabled or notifier is None:
        return False
    payload: Dict[str, Any] = {"message": str(message)}
    if reason is not None:
        payload["reason"] = str(reason)
    try:
        return bool(notifier.emit(event_type, payload))
    except Exception:
        return False


def run_pre_execution_gate(
    *,
    settings: PreExecutionSettings,
    evidence_dir: Path,
    notifier: Any = None,
    run_cmd: Callable[..., Any] = subprocess.run,
    connect_fn: Callable[..., Any] = socket.create_connection,
    sleep_fn: Callable[[float], None] = time.sleep,
    backend_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    gate_dir = evidence_dir / "pre_execution"
    gate_dir.mkdir(parents=True, exist_ok=True)
    if not settings.enabled:
        out = {"enabled": False, "ready": False, "reason": "pre_execution_disabled"}
        _write_json(gate_dir / "pre_execution.json", out)
        return out

    try:
        tws = run_tws_e2e(
            script_path=settings.tws_e2e_script,
            timeout_sec=settings.tws_e2e_timeout_sec,
            env_passthrough=settings.tws_e2e_env_passthrough,
            evidence_dir=gate_dir,
            run_cmd=run_cmd,
        )
        port = check_port_open(
            cfg=settings.port_check,
            evidence_dir=gate_dir,
            connect_fn=connect_fn,
            sleep_fn=sleep_fn,
        )
        handshake = broker_handshake_read_only(
            cfg=settings.handshake,
            host=settings.port_check.host,
            port=settings.port_check.port,
            client_id=settings.ibkr_client_id,
            evidence_dir=gate_dir,
            backend_factory=backend_factory,
        )
        out = {
            "enabled": True,
            "ready": True,
            "reason": "pre_execution_ready",
            "tws_e2e": tws,
            "port_check": {"ready": True, "host": settings.port_check.host, "port": settings.port_check.port, "attempts": len(port.get("attempts", []))},
            "handshake": {"ready": bool(handshake.get("ready", False)), "seen_events": list(handshake.get("seen_events", []))},
        }
        _write_json(gate_dir / "pre_execution.json", out)
        _notify(
            notifier=notifier,
            telegram_cfg=settings.telegram,
            event_type="pre_execution_ready",
            message=settings.telegram.on_ready_message,
        )
        return out
    except PreExecutionError as exc:
        out = {"enabled": True, "ready": False, "reason": exc.reason, "detail": exc.detail}
        _write_json(gate_dir / "pre_execution.json", out)
        _notify(
            notifier=notifier,
            telegram_cfg=settings.telegram,
            event_type="pre_execution_failed",
            message=settings.telegram.on_fail_message.format(reason=exc.reason),
            reason=exc.reason,
        )
        raise
