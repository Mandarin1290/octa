from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import re
import socket
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

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
    # Mirrors tws_e2e.sh api_handshake_try() behaviour:
    ports: tuple[int, ...] = ()           # empty = use port from PortCheckConfig
    use_random_client_id: bool = True     # random in [2000, 65000] to avoid conflicts
    require_managed_accounts: bool = True # ib.managedAccounts() must return non-empty


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


# ---------------------------------------------------------------------------
# Evidence helpers — LOG: parsing, env-snapshot, secret redaction
# ---------------------------------------------------------------------------

_SECRET_PAT: re.Pattern[str] = re.compile(
    r"(?i)(password|passwd|secret|token|api[_\-]?key|credential|private[_\-]?key|auth|username|account|user)"
)


def _parse_log_path(stdout: str) -> Optional[str]:
    """Extract the path emitted by tws_e2e.sh as 'LOG: <path>'."""
    m = re.search(r"(?m)^LOG:\s*(\S+)", str(stdout))
    return m.group(1) if m else None


def _redact_env(env: Dict[str, str]) -> Dict[str, str]:
    """Return a copy of *env* with secret values replaced by '<redacted>'."""
    return {k: ("<redacted>" if _SECRET_PAT.search(k) else v) for k, v in env.items()}


def _write_env_snapshot(path: Path, env_passthrough: bool) -> None:
    """Write a redacted environment snapshot to *path*."""
    if env_passthrough:
        raw_env: Dict[str, str] = dict(os.environ)
    else:
        raw_env = {k: os.environ.get(k, "") for k in ("HOME", "PATH")}
    redacted = _redact_env(raw_env)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(f"{k}={v}" for k, v in sorted(redacted.items())) + "\n",
        encoding="utf-8",
    )


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

    ports_raw = hs.get("ports", [])
    hs_ports: tuple[int, ...] = (
        tuple(int(p) for p in ports_raw) if isinstance(ports_raw, list) and ports_raw else ()
    )

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
            connect_timeout_sec=float(hs.get("connect_timeout_sec", 4)),
            wait_for=wait_for,
            overall_timeout_sec=float(hs.get("overall_timeout_sec", 15)),
            ports=hs_ports,
            use_random_client_id=bool(hs.get("use_random_client_id", True)),
            require_managed_accounts=bool(hs.get("require_managed_accounts", True)),
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
    cmd = ["bash", str(script)]
    # Write the exact command before execution so it is available even on crash.
    (evidence_dir / "cmd.txt").write_text(" ".join(cmd) + "\n", encoding="utf-8")
    cp = run_cmd(
        cmd,
        capture_output=True,
        text=True,
        timeout=int(timeout_sec),
        env=env,
        check=False,
    )
    stdout = str(cp.stdout or "")
    stderr = str(cp.stderr or "")
    (evidence_dir / "tws_e2e.stdout.log").write_text(stdout, encoding="utf-8")
    (evidence_dir / "tws_e2e.stderr.log").write_text(stderr, encoding="utf-8")
    log_path = _parse_log_path(stdout)
    out = {
        "returncode": int(cp.returncode),
        "script": str(script),
        "timeout_sec": int(timeout_sec),
        "log_path": log_path,
    }
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
    def __init__(
        self,
        *,
        host: str,
        port: int,
        client_id: int,
        connect_timeout_sec: float,
        ports: tuple[int, ...] = (),
    ) -> None:
        try:
            from ib_insync import IB  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise PreExecutionError("pre_execution_handshake_failed", f"ib_insync_missing:{exc}") from exc
        self._ib = IB()
        self._host = str(host)
        # Primary port is the first element of ports (if provided) or the explicit port arg.
        self._ports: tuple[int, ...] = ports if ports else (int(port),)
        self._active_port: int = self._ports[0]
        self._client_id = int(client_id)
        self._connect_timeout_sec = float(connect_timeout_sec)
        self.forbidden_calls: List[str] = []

    def connect(self) -> bool:
        """Try each port in order; return True on first successful connection."""
        for port in self._ports:
            try:
                ok = self._ib.connect(
                    self._host, port, clientId=self._client_id, timeout=self._connect_timeout_sec
                )
                if ok and self._ib.isConnected():
                    self._active_port = port
                    return True
            except Exception:
                pass
            # Ensure clean state before next port attempt.
            try:
                self._ib.disconnect()
            except Exception:
                pass
        return False

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

    def req_managed_accounts(self) -> bool:
        """Return True when ib.managedAccounts() yields at least one account.

        Mirrors tws_e2e.sh api_handshake_try(): ``acc = ib.managedAccounts()``
        must be truthy before the handshake is declared successful.
        """
        try:
            accounts = self._ib.managedAccounts()
            return bool(accounts)
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

    # Effective port list: cfg.ports (multi-port) overrides the single port arg.
    effective_ports: tuple[int, ...] = cfg.ports if cfg.ports else (int(port),)

    # Random client_id avoids conflicts with running sessions (mirrors tws_e2e.sh).
    effective_client_id = (
        random.randint(2000, 65000) if cfg.use_random_client_id else int(client_id)
    )

    factory = backend_factory or (
        lambda **kw: IBInsyncReadOnlyBackend(
            host=kw["host"],
            port=kw["port"],
            client_id=kw["client_id"],
            connect_timeout_sec=kw["connect_timeout_sec"],
            ports=kw.get("ports", ()),
        )
    )
    backend = factory(
        host=str(host),
        port=int(effective_ports[0]),
        client_id=int(effective_client_id),
        connect_timeout_sec=float(cfg.connect_timeout_sec),
        ports=effective_ports,
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

        # managedAccounts() confirms an authenticated session, not just a connection
        # (mirrors tws_e2e.sh: ``acc = ib.managedAccounts()`` must be truthy).
        managed_accounts_ok = False
        if cfg.require_managed_accounts:
            req_fn = getattr(backend, "req_managed_accounts", None)
            if callable(req_fn):
                managed_accounts_ok = bool(req_fn())
            if not managed_accounts_ok:
                raise PreExecutionError(
                    "pre_execution_handshake_failed", "managed_accounts_empty"
                )
            seen_events.append("managedAccounts")

        forbidden_calls = list(getattr(backend, "forbidden_calls", []) or [])
        if forbidden_calls:
            raise PreExecutionError("pre_execution_handshake_not_read_only", ",".join(forbidden_calls))

        # Resolve which port actually connected (IBInsyncReadOnlyBackend tracks it).
        active_port = int(getattr(backend, "_active_port", effective_ports[0]))
        out = {
            "enabled": True,
            "mode": cfg.mode,
            "ready": True,
            "seen_events": seen_events,
            "managed_accounts_ok": managed_accounts_ok,
            "host": str(host),
            "port": active_port,
            "client_id": int(effective_client_id),
            "use_random_client_id": bool(cfg.use_random_client_id),
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
    mode: str = "paper",
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    gate_dir = evidence_dir / "pre_execution"
    gate_dir.mkdir(parents=True, exist_ok=True)
    effective_run_id = run_id or evidence_dir.name
    started_at = datetime.now(timezone.utc).isoformat()

    if not settings.enabled:
        out = {"enabled": False, "ready": False, "reason": "pre_execution_disabled"}
        _write_json(gate_dir / "pre_execution.json", out)
        return out

    # Write env snapshot immediately — available even if later gates abort.
    _write_env_snapshot(gate_dir / "env_snapshot.txt", settings.tws_e2e_env_passthrough)

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
        completed_at = datetime.now(timezone.utc).isoformat()
        script_log_path = tws.get("log_path")

        # preexec_manifest.json — stable config hash for audit cross-reference.
        config_hash = hashlib.sha256(
            json.dumps(
                {
                    "api_host": settings.port_check.host,
                    "api_port": settings.port_check.port,
                    "mode": mode,
                    "tws_e2e_script": settings.tws_e2e_script,
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        manifest: Dict[str, Any] = {
            "run_id": effective_run_id,
            "mode": mode,
            "started_at": started_at,
            "completed_at": completed_at,
            "tws_e2e_script": settings.tws_e2e_script,
            "api_host": settings.port_check.host,
            "api_port": settings.port_check.port,
            "config_hash": config_hash,
            "failed": False,
        }
        _write_json(gate_dir / "preexec_manifest.json", manifest)

        # result.json — composite summary (rc, handshake_ok, log path, …).
        result_payload: Dict[str, Any] = {
            "rc": int(tws.get("returncode", -1)),
            "handshake_ok": bool(handshake.get("ready", False)),
            "api_host": settings.port_check.host,
            "api_port": int(settings.port_check.port),
            "script_log_path": script_log_path,
            "tws_main_window_id": None,
            "mode": mode,
            "run_id": effective_run_id,
            "started_at": started_at,
            "completed_at": completed_at,
        }
        _write_json(gate_dir / "result.json", result_payload)

        out = {
            "enabled": True,
            "ready": True,
            "reason": "pre_execution_ready",
            "tws_e2e": tws,
            "port_check": {
                "ready": True,
                "host": settings.port_check.host,
                "port": settings.port_check.port,
                "attempts": len(port.get("attempts", [])),
            },
            "handshake": {
                "ready": bool(handshake.get("ready", False)),
                "seen_events": list(handshake.get("seen_events", [])),
            },
            "run_id": effective_run_id,
            "mode": mode,
        }
        _write_json(gate_dir / "pre_execution.json", out)

        # Telegram success message with metadata.
        log_suffix = f"\nlog: {script_log_path}" if script_log_path else ""
        ready_msg = (
            f"{settings.telegram.on_ready_message}"
            f"\nmode={mode}  run_id={effective_run_id}"
            f"  port={settings.port_check.port}{log_suffix}"
        )
        _notify(
            notifier=notifier,
            telegram_cfg=settings.telegram,
            event_type="pre_execution_ready",
            message=ready_msg,
        )
        return out
    except PreExecutionError as exc:
        completed_at = datetime.now(timezone.utc).isoformat()
        # Write partial manifest so auditors know a run was attempted.
        _write_json(
            gate_dir / "preexec_manifest.json",
            {
                "run_id": effective_run_id,
                "mode": mode,
                "started_at": started_at,
                "completed_at": completed_at,
                "tws_e2e_script": settings.tws_e2e_script,
                "api_host": settings.port_check.host,
                "api_port": settings.port_check.port,
                "failed": True,
                "failure_reason": exc.reason,
                "failure_detail": exc.detail,
            },
        )
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
