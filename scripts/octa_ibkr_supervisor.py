#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import shlex
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from octa.execution.x11_preflight import run_full_preflight


EXIT_OK = 0
EXIT_PROCESS_NOT_RUNNING = 2
EXIT_PORT_CLOSED = 3
EXIT_API_PROBE_FAILED = 4
EXIT_MISCONFIG = 5


@dataclass(frozen=True)
class SupervisorConfig:
    config_path: Path
    host: str
    port: int
    mode: str
    process_match: str
    client_id: int
    timeout_sec: float
    config_present: bool
    generated_config_path: Path | None
    launch_providers: list[dict[str, Any]]


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _redact_env(env_map: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in sorted(env_map.items()):
        upper = key.upper()
        if any(tok in upper for tok in ("PASS", "SECRET", "TOKEN", "KEY", "USER", "ACCOUNT", "AUTH")):
            out[key] = "<REDACTED>"
        else:
            out[key] = str(value)
    return out


def _normalize_launch_providers(raw: Any, *, mode: str) -> list[dict[str, Any]]:
    if not isinstance(raw, dict):
        return []
    providers = raw.get("providers")
    if isinstance(providers, list):
        out: list[dict[str, Any]] = []
        for row in providers:
            if isinstance(row, dict):
                out.append(dict(row))
        return out
    if isinstance(raw.get("provider"), str):
        return [dict(raw)]
    return []


def _default_launch_providers(mode: str) -> list[dict[str, Any]]:
    env_cmd = os.environ.get("OCTA_GATEWAY_CMD") if mode == "gateway" else os.environ.get("OCTA_TWS_CMD")
    providers: list[dict[str, Any]] = []
    if env_cmd:
        providers.append(
            {
                "provider": "direct",
                "command": shlex.split(env_cmd),
                "cwd": str(Path.cwd()),
                "log_file": "logs/ibkr_broker.log",
                "env": {},
            }
        )
    providers.append({"provider": "systemd-user", "unit": "octa-ibkr.service"})
    return providers


def _load_config(path: Path, evidence_dir: Path) -> SupervisorConfig:
    default_host = str(os.environ.get("OCTA_IBKR_HOST") or "127.0.0.1")
    default_port = _safe_int(os.environ.get("OCTA_IBKR_PORT"), 7497)
    default_mode = str(os.environ.get("OCTA_IBKR_MODE") or "tws").strip().lower()
    if default_mode not in {"tws", "gateway"}:
        default_mode = "tws"
    default_match = str(os.environ.get("OCTA_IBKR_PROCESS_MATCH") or ("ibgateway" if default_mode == "gateway" else "tws"))
    default_client_id = _safe_int(os.environ.get("OCTA_IBKR_CLIENT_ID"), 901)
    default_timeout = _safe_float(os.environ.get("OCTA_IBKR_TIMEOUT"), 5.0)

    if not path.exists():
        generated = evidence_dir / "generated_execution_ibkr.yaml"
        _write_text(
            generated,
            yaml.safe_dump(
                {
                    "ibkr": {
                        "host": default_host,
                        "port": default_port,
                        "client_id": default_client_id,
                        "mode": default_mode,
                        "process_match": default_match,
                        "timeout": default_timeout,
                    },
                    "launch": {
                        "providers": _default_launch_providers(default_mode),
                    },
                },
                sort_keys=True,
            ),
        )
        return SupervisorConfig(
            config_path=path,
            host=default_host,
            port=default_port,
            mode=default_mode,
            process_match=default_match,
            client_id=default_client_id,
            timeout_sec=default_timeout,
            config_present=False,
            generated_config_path=generated,
            launch_providers=_default_launch_providers(default_mode),
        )

    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise ValueError(f"config_parse_error:{type(exc).__name__}:{exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("config_parse_error:root_not_mapping")

    root = data.get("ibkr") if isinstance(data.get("ibkr"), dict) else data
    host = str(root.get("host") or root.get("hostname") or default_host)
    port = _safe_int(root.get("port") or root.get("api_port") or root.get("tws_port"), default_port)
    mode = str(root.get("mode") or default_mode).strip().lower()
    if mode not in {"tws", "gateway"}:
        raise ValueError(f"config_parse_error:invalid_mode:{mode}")
    mode_default_match = "ibgateway" if mode == "gateway" else "tws"
    process_match = str(
        root.get("process_match")
        or root.get("process_match_substring")
        or mode_default_match
    )
    client_id = _safe_int(root.get("client_id") or root.get("clientId"), default_client_id)
    timeout_sec = _safe_float(root.get("timeout"), default_timeout)
    launch_providers = _normalize_launch_providers(data.get("launch"), mode=mode)
    if not launch_providers:
        launch_providers = _default_launch_providers(mode)

    return SupervisorConfig(
        config_path=path,
        host=host,
        port=port,
        mode=mode,
        process_match=process_match,
        client_id=client_id,
        timeout_sec=timeout_sec,
        config_present=True,
        generated_config_path=None,
        launch_providers=launch_providers,
    )


def _record_command(rec: list[str], argv: list[str]) -> None:
    rec.append(" ".join(shlex.quote(x) for x in argv))


def _run_cmd(rec: list[str], argv: list[str], *, timeout: float = 20.0, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    _record_command(rec, argv)
    return subprocess.run(argv, capture_output=True, text=True, check=False, timeout=timeout, env=env)


def _process_running(pattern: str) -> tuple[bool, list[str]]:
    cp = subprocess.run(["pgrep", "-fa", pattern], capture_output=True, text=True, check=False)
    if cp.returncode != 0:
        return False, []
    lines = []
    for line in (cp.stdout or "").splitlines():
        text = line.strip()
        if not text:
            continue
        if "octa_ibkr_supervisor.py" in text:
            continue
        if "pgrep -fa" in text:
            continue
        lines.append(text)
    return bool(lines), lines


def _port_open(host: str, port: int, timeout: float = 0.8) -> tuple[bool, str]:
    try:
        sock = socket.socket()
    except Exception as exc:
        return False, f"{type(exc).__name__}:{exc}"
    sock.settimeout(timeout)
    try:
        sock.connect((host, int(port)))
        return True, "ok"
    except Exception as exc:
        return False, f"{type(exc).__name__}:{exc}"
    finally:
        try:
            sock.close()
        except Exception:
            pass


def _systemd_unit_present(repo_root: Path) -> bool:
    return (repo_root / "systemd" / "octa-ibkr.service").exists()


def _command_available(name: str) -> bool:
    cp = subprocess.run(["bash", "-lc", f"command -v {shlex.quote(name)}"], capture_output=True, text=True, check=False)
    return cp.returncode == 0


def _cmd_exists(command: list[str]) -> tuple[bool, str]:
    if not command:
        return False, "empty_command"
    exe = str(command[0])
    if os.path.isabs(exe):
        p = Path(exe)
        if p.exists() and os.access(str(p), os.X_OK):
            return True, str(p)
        return False, f"missing_executable:{exe}"
    resolved = shutil.which(exe)
    if resolved:
        return True, resolved
    return False, f"missing_executable:{exe}"


def _start_ibkr(
    *,
    rec: list[str],
    repo_root: Path,
    cfg: SupervisorConfig,
    evidence_dir: Path,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    env = dict(os.environ)
    env["OCTA_REPO"] = str(repo_root)
    env.setdefault("OCTA_IBKR_HOST", cfg.host)
    env.setdefault("OCTA_IBKR_PORT", str(cfg.port))
    env.setdefault("OCTA_IBKR_MODE", cfg.mode)
    env.setdefault("OCTA_IBKR_PROCESS_MATCH", cfg.process_match)
    env.setdefault("OCTA_XVFB_DISPLAY", ":99")
    env.setdefault("OCTA_DISPLAY", env.get("OCTA_XVFB_DISPLAY", ":99"))
    env.setdefault("DISPLAY", env.get("OCTA_DISPLAY", ":99"))

    providers = list(cfg.launch_providers)
    for idx, provider in enumerate(providers, start=1):
        kind = str(provider.get("provider") or "").strip().lower()
        if not kind:
            attempts.append({"provider_index": idx, "provider": "invalid", "rc": 1, "reason": "missing_provider"})
            continue

        if kind == "systemd-user":
            unit = str(provider.get("unit") or "octa-ibkr.service")
            if not _command_available("systemctl"):
                attempts.append({"provider_index": idx, "provider": kind, "unit": unit, "rc": 1, "reason": "systemctl_missing"})
                continue
            cp = _run_cmd(rec, ["systemctl", "--user", "start", unit], timeout=20.0, env=env)
            attempts.append(
                {
                    "provider_index": idx,
                    "provider": kind,
                    "unit": unit,
                    "rc": int(cp.returncode),
                    "stdout": (cp.stdout or "").strip(),
                    "stderr": (cp.stderr or "").strip(),
                }
            )
            if cp.returncode == 0:
                return {"started": True, "method": kind, "attempts": attempts}
            continue

        if kind == "systemd-system":
            unit = str(provider.get("unit") or "octa-ibkr.service")
            if not _command_available("sudo") or not _command_available("systemctl"):
                attempts.append({"provider_index": idx, "provider": kind, "unit": unit, "rc": 1, "reason": "sudo_or_systemctl_missing"})
                continue
            cp = _run_cmd(rec, ["sudo", "-n", "systemctl", "start", unit], timeout=20.0, env=env)
            attempts.append(
                {
                    "provider_index": idx,
                    "provider": kind,
                    "unit": unit,
                    "rc": int(cp.returncode),
                    "stdout": (cp.stdout or "").strip(),
                    "stderr": (cp.stderr or "").strip(),
                }
            )
            if cp.returncode == 0:
                return {"started": True, "method": kind, "attempts": attempts}
            continue

        if kind == "direct":
            command = provider.get("command")
            cmd_list = [str(x) for x in command] if isinstance(command, list) else []
            ok_cmd, cmd_detail = _cmd_exists(cmd_list)
            provider_env = provider.get("env") if isinstance(provider.get("env"), dict) else {}
            merged_env = dict(env)
            for k, v in provider_env.items():
                merged_env[str(k)] = str(v)
            cwd_raw = provider.get("cwd")
            cwd = Path(str(cwd_raw)).expanduser() if cwd_raw else repo_root
            log_raw = str(provider.get("log_file") or "logs/ibkr_broker.log")
            log_path = Path(log_raw)
            if not log_path.is_absolute():
                log_path = repo_root / log_path
            log_path.parent.mkdir(parents=True, exist_ok=True)
            if not ok_cmd:
                attempts.append(
                    {
                        "provider_index": idx,
                        "provider": kind,
                        "rc": 1,
                        "reason": cmd_detail,
                        "command": cmd_list,
                        "cwd": str(cwd),
                        "log_file": str(log_path),
                    }
                )
                continue
            _record_command(rec, cmd_list)
            try:
                with log_path.open("a", encoding="utf-8") as logf:
                    proc = subprocess.Popen(  # noqa: S603
                        cmd_list,
                        stdout=logf,
                        stderr=logf,
                        stdin=subprocess.DEVNULL,
                        env=merged_env,
                        cwd=str(cwd),
                        start_new_session=True,
                    )
                time.sleep(1.0)
                alive, _lines = _process_running(cfg.process_match)
                attempts.append(
                    {
                        "provider_index": idx,
                        "provider": kind,
                        "rc": 0 if alive else 2,
                        "pid": int(proc.pid),
                        "process_seen": bool(alive),
                        "resolved_executable": cmd_detail,
                        "command": cmd_list,
                        "cwd": str(cwd),
                        "log_file": str(log_path),
                        "env": _redact_env({k: str(v) for k, v in provider_env.items()}),
                    }
                )
                if alive:
                    return {"started": True, "method": kind, "attempts": attempts}
            except Exception as exc:
                attempts.append(
                    {
                        "provider_index": idx,
                        "provider": kind,
                        "rc": 1,
                        "error": f"{type(exc).__name__}:{exc}",
                        "command": cmd_list,
                        "cwd": str(cwd),
                        "log_file": str(log_path),
                        "env": _redact_env({k: str(v) for k, v in provider_env.items()}),
                    }
                )
            continue

        attempts.append({"provider_index": idx, "provider": kind, "rc": 1, "reason": "unsupported_provider"})

    runtime_cmd = [
        sys.executable,
        "-m",
        "octa.execution.ibkr_runtime",
        "--mode",
        cfg.mode,
        "--process-match",
        cfg.process_match,
        "--host",
        cfg.host,
        "--port",
        str(cfg.port),
        "--ensure-running",
    ]
    cp = _run_cmd(rec, runtime_cmd, timeout=20.0, env=env)
    attempts.append(
        {
            "method": "ibkr_runtime_ensure_running",
            "rc": int(cp.returncode),
            "stdout": (cp.stdout or "").strip(),
            "stderr": (cp.stderr or "").strip(),
        }
    )
    return {"started": cp.returncode == 0, "method": "ibkr_runtime_ensure_running", "attempts": attempts}


def _load_smoke_probe_code(repo_root: Path) -> str:
    smoke_path = repo_root / "scripts" / "octa_smoke_chain.py"
    if not smoke_path.exists():
        raise RuntimeError("missing_smoke_chain_script")
    spec = importlib.util.spec_from_file_location("octa_smoke_chain", smoke_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable_to_load_smoke_chain_script")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fn = getattr(module, "_ibkr_connect_async_probe_code", None)
    if not callable(fn):
        raise RuntimeError("smoke_chain_probe_function_missing")
    code = str(fn())
    if "connectAsync" not in code:
        raise RuntimeError("unexpected_probe_code_content")
    return code


def _run_api_probe(
    *,
    rec: list[str],
    repo_root: Path,
    probe_config_path: Path,
    timeout_sec: float,
) -> dict[str, Any]:
    code = _load_smoke_probe_code(repo_root)
    cmd = [sys.executable, "-c", code, str(probe_config_path)]
    cp = _run_cmd(rec, cmd, timeout=max(10.0, timeout_sec + 5.0))
    payload: dict[str, Any] = {}
    try:
        payload = json.loads((cp.stdout or "").strip() or "{}")
    except Exception:
        payload = {}
    api_ready = bool(cp.returncode == 0 and "serverVersion" in payload and "managedAccounts" in payload)
    return {
        "rc": int(cp.returncode),
        "stdout": cp.stdout or "",
        "stderr": cp.stderr or "",
        "payload": payload,
        "api_ready": api_ready,
    }


def _write_sha_manifest(evidence_dir: Path) -> None:
    rows: list[str] = []
    for p in sorted(evidence_dir.rglob("*")):
        if not p.is_file():
            continue
        if p.name == "sha256sum.txt":
            continue
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        rel = p.relative_to(evidence_dir).as_posix()
        rows.append(f"{digest}  {rel}")
    _write_text(evidence_dir / "sha256sum.txt", "\n".join(rows) + ("\n" if rows else ""))


def main() -> int:
    raise SystemExit(
        "non_canonical_broker_supervisor:scripts/octa_ibkr_supervisor.py:"
        "broker_supervision_is_not_part_of_v0_0_0_foundation_scope"
    )

    ap = argparse.ArgumentParser(description="OCTA IBKR bring-up supervisor (paper/shadow-safe, no-skip probe).")
    ap.add_argument("--config", default="configs/execution_ibkr.yaml")
    ap.add_argument("--timeout-sec", type=int, default=30)
    ap.add_argument("--poll-sec", type=int, default=1)
    ap.add_argument("--no-skip", action="store_true", default=False, help="Compatibility flag; probe is always no-skip.")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    stamp = _utc_stamp()
    evidence_dir = repo_root / "octa" / "var" / "evidence" / f"ibkr_supervisor_{stamp}"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    commands: list[str] = []

    config_error = None
    try:
        cfg = _load_config(Path(args.config), evidence_dir)
    except ValueError as exc:
        config_error = str(exc)
        cfg = None

    if cfg is None:
        health = {
            "timestamp_utc": _utc_iso(),
            "verdict": "NO_GO",
            "exit_code": EXIT_MISCONFIG,
            "classification": "MISCONFIG",
            "reason": config_error or "unknown_config_error",
        }
        _write_json(evidence_dir / "health.json", health)
        _write_text(evidence_dir / "commands.txt", "")
        _write_text(evidence_dir / "poll_log.jsonl", "")
        _write_text(
            evidence_dir / "report.md",
            "# OCTA IBKR Supervisor Report\n\n"
            f"- timestamp_utc: `{_utc_iso()}`\n"
            f"- verdict: `NO_GO`\n"
            f"- classification: `MISCONFIG`\n"
            f"- reason: `{config_error}`\n",
        )
        _write_sha_manifest(evidence_dir)
        print(json.dumps(health, sort_keys=True))
        return EXIT_MISCONFIG

    if str(cfg.mode).lower() == "tws":
        x11 = run_full_preflight(evidence_root=repo_root / "octa" / "var" / "evidence")
        if not bool(x11.get("ok")):
            code = int(x11.get("code", 11))
            reason = str(x11.get("reason", "X_SERVER_UNREACHABLE"))
            health = {
                "timestamp_utc": _utc_iso(),
                "verdict": "NO_GO",
                "exit_code": code,
                "classification": reason,
                "reason": "x11_preflight_failed",
                "x11_preflight": x11,
                "safety": {
                    "live_enabled": False,
                    "paper_shadow_only": True,
                    "fail_closed": True,
                    "no_skip_probe": True,
                },
            }
            _write_json(evidence_dir / "health.json", health)
            _write_text(evidence_dir / "commands.txt", "")
            _write_text(evidence_dir / "poll_log.jsonl", "")
            report = [
                "# OCTA IBKR Supervisor Report",
                "",
                f"- timestamp_utc: `{health['timestamp_utc']}`",
                f"- verdict: `{health['verdict']}`",
                f"- classification: `{reason}`",
                f"- exit_code: `{code}`",
                f"- x11_preflight_evidence: `{x11.get('evidence_dir', '')}`",
            ]
            _write_text(evidence_dir / "report.md", "\n".join(report) + "\n")
            _write_sha_manifest(evidence_dir)
            print(f"❌ X11 preflight failed: {reason}")
            print("Action:")
            for action in x11.get("actions", []):
                print(f"  - {action}")
            return code

    poll_log_path = evidence_dir / "poll_log.jsonl"
    poll_file = poll_log_path.open("w", encoding="utf-8")

    initial_process, initial_lines = _process_running(cfg.process_match)
    initial_port, initial_port_reason = _port_open(cfg.host, cfg.port)

    start_result: dict[str, Any] = {"started": False, "method": "none", "attempts": []}
    if not (initial_process and initial_port):
        start_result = _start_ibkr(rec=commands, repo_root=repo_root, cfg=cfg, evidence_dir=evidence_dir)

    deadline = time.monotonic() + max(1, int(args.timeout_sec))
    poll_index = 0
    final_process = initial_process
    final_lines = initial_lines
    final_port = initial_port
    final_port_reason = initial_port_reason
    while True:
        poll_index += 1
        final_process, final_lines = _process_running(cfg.process_match)
        final_port, final_port_reason = _port_open(cfg.host, cfg.port)
        row = {
            "ts_utc": _utc_iso(),
            "poll_index": poll_index,
            "process_running": bool(final_process),
            "process_match": cfg.process_match,
            "process_lines": final_lines,
            "port_open": bool(final_port),
            "host": cfg.host,
            "port": cfg.port,
            "port_reason": final_port_reason,
        }
        poll_file.write(json.dumps(row, sort_keys=True) + "\n")
        poll_file.flush()
        if final_process and final_port:
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(max(1, int(args.poll_sec)))
    poll_file.close()

    probe_config = cfg.generated_config_path or cfg.config_path
    api_probe: dict[str, Any] = {"api_ready": False, "rc": None, "stdout": "", "stderr": "", "payload": {}}
    exit_code = EXIT_OK
    classification = "API_READY"
    reasons: list[str] = []

    if not final_process:
        exit_code = EXIT_PROCESS_NOT_RUNNING
        classification = "PROCESS_NOT_RUNNING"
        reasons.append("broker_process_not_running_after_start_attempt")
    elif not final_port:
        exit_code = EXIT_PORT_CLOSED
        classification = "PORT_CLOSED"
        reasons.append("api_port_closed_after_start_attempt")
    else:
        try:
            api_probe = _run_api_probe(rec=commands, repo_root=repo_root, probe_config_path=probe_config, timeout_sec=cfg.timeout_sec)
        except Exception as exc:
            api_probe = {"api_ready": False, "rc": 1, "stdout": "", "stderr": f"{type(exc).__name__}:{exc}", "payload": {}}
        if not bool(api_probe.get("api_ready")):
            exit_code = EXIT_API_PROBE_FAILED
            classification = "API_PROBE_FAILED"
            reasons.append("process_and_port_ready_but_api_probe_not_ready")

    health = {
        "timestamp_utc": _utc_iso(),
        "verdict": "GO" if exit_code == EXIT_OK else "NO_GO",
        "exit_code": int(exit_code),
        "classification": classification,
        "config": {
            "requested_path": str(cfg.config_path),
            "config_present": bool(cfg.config_present),
            "probe_config_path": str(probe_config),
            "mode": cfg.mode,
            "host": cfg.host,
            "port": cfg.port,
            "process_match": cfg.process_match,
            "client_id": cfg.client_id,
            "timeout_sec": cfg.timeout_sec,
            "launch_providers": cfg.launch_providers,
        },
        "initial_state": {
            "process_running": bool(initial_process),
            "process_lines": initial_lines,
            "port_open": bool(initial_port),
            "port_reason": initial_port_reason,
        },
        "start": start_result,
        "final_state": {
            "process_running": bool(final_process),
            "process_lines": final_lines,
            "port_open": bool(final_port),
            "port_reason": final_port_reason,
        },
        "api_probe": {
            "api_ready": bool(api_probe.get("api_ready", False)),
            "rc": api_probe.get("rc"),
            "payload": api_probe.get("payload", {}),
            "stderr_tail": (api_probe.get("stderr", "") or "")[-2000:],
        },
        "reasons": reasons,
        "safety": {
            "live_enabled": False,
            "paper_shadow_only": True,
            "fail_closed": True,
            "no_skip_probe": True,
        },
    }
    _write_json(evidence_dir / "health.json", health)
    _write_text(evidence_dir / "commands.txt", "\n".join(commands) + ("\n" if commands else ""))

    report_lines = [
        "# OCTA IBKR Supervisor Report",
        "",
        f"- timestamp_utc: `{health['timestamp_utc']}`",
        f"- verdict: `{health['verdict']}`",
        f"- classification: `{classification}`",
        f"- exit_code: `{exit_code}`",
        f"- config_path: `{cfg.config_path}`",
        f"- probe_config_path: `{probe_config}`",
        f"- host_port: `{cfg.host}:{cfg.port}`",
        f"- process_match: `{cfg.process_match}`",
        f"- start_method: `{start_result.get('method', 'none')}`",
        f"- api_ready: `{bool(api_probe.get('api_ready', False))}`",
        "",
        "## Actionable Reasons",
    ]
    for reason in reasons or ["none"]:
        report_lines.append(f"- {reason}")
    _write_text(evidence_dir / "report.md", "\n".join(report_lines) + "\n")

    _write_sha_manifest(evidence_dir)
    print(json.dumps({"evidence_dir": str(evidence_dir.relative_to(repo_root)), "exit_code": int(exit_code), "classification": classification}, sort_keys=True))
    return int(exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
