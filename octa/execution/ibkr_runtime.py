from __future__ import annotations

import json
import shlex
import socket
import subprocess
import time
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class IBKRRuntimeConfig:
    mode: str = "tws"  # tws|gateway
    tws_cmd: list[str] | None = None
    gateway_cmd: list[str] | None = None
    process_match_substring: str | None = None
    host: str | None = None
    port: int | None = None


def _split_cmd(raw: str | None) -> list[str]:
    if not raw:
        return []
    return shlex.split(str(raw))


def _is_running(match_substring: str) -> bool:
    cp = subprocess.run(["pgrep", "-fa", str(match_substring)], capture_output=True, text=True, check=False)
    if cp.returncode != 0:
        return False
    return bool((cp.stdout or "").strip())


def _probe_port(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def ensure_ibkr_running(cfg: IBKRRuntimeConfig) -> dict[str, Any]:
    mode = str(cfg.mode).strip().lower()
    if mode not in {"tws", "gateway"}:
        return {"ok": False, "error": "invalid_mode", "mode": mode}

    cmd = list(cfg.tws_cmd or []) if mode == "tws" else list(cfg.gateway_cmd or [])
    if not cmd:
        return {"ok": False, "error": "missing_launch_command", "mode": mode}

    match_substring = str(cfg.process_match_substring or cmd[0])
    if _is_running(match_substring):
        return {"ok": True, "mode": mode, "action": "already_running", "match": match_substring}

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as exc:
        return {"ok": False, "mode": mode, "action": "launch_failed", "error": f"{type(exc).__name__}: {exc}"}

    time.sleep(0.5)
    running = _is_running(match_substring)
    return {
        "ok": bool(running),
        "mode": mode,
        "action": "launched" if running else "launch_unconfirmed",
        "pid": int(proc.pid),
        "match": match_substring,
    }


def ibkr_health(cfg: IBKRRuntimeConfig) -> dict[str, Any]:
    mode = str(cfg.mode).strip().lower()
    cmd = list(cfg.tws_cmd or []) if mode == "tws" else list(cfg.gateway_cmd or [])
    match_substring = str(cfg.process_match_substring or (cmd[0] if cmd else mode))
    process_alive = _is_running(match_substring)

    port_ok = None
    if cfg.host is not None and cfg.port is not None:
        port_ok = _probe_port(str(cfg.host), int(cfg.port), timeout=1.0)

    return {
        "mode": mode,
        "process_alive": bool(process_alive),
        "process_match": match_substring,
        "host": cfg.host,
        "port": cfg.port,
        "port_reachable": port_ok,
        "healthy": bool(process_alive and (True if port_ok is None else bool(port_ok))),
    }


def disconnect_indicators(cfg: IBKRRuntimeConfig) -> dict[str, Any]:
    health = ibkr_health(cfg)
    reasons: list[str] = []
    if not bool(health.get("process_alive")):
        reasons.append("process_dead")
    if health.get("port_reachable") is False:
        reasons.append("api_port_unreachable")
    return {"disconnect": bool(reasons), "reasons": reasons, "health": health}


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="IBKR runtime launcher and health probe")
    ap.add_argument("--mode", default="tws", choices=["tws", "gateway"])
    ap.add_argument("--tws-cmd", default="")
    ap.add_argument("--gateway-cmd", default="")
    ap.add_argument("--process-match", default="")
    ap.add_argument("--host", default="")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--ensure-running", action="store_true", default=False)
    ap.add_argument("--health", action="store_true", default=False)
    args = ap.parse_args()

    cfg = IBKRRuntimeConfig(
        mode=str(args.mode),
        tws_cmd=_split_cmd(args.tws_cmd),
        gateway_cmd=_split_cmd(args.gateway_cmd),
        process_match_substring=str(args.process_match or "") or None,
        host=str(args.host or "") or None,
        port=int(args.port) if int(args.port) > 0 else None,
    )

    if args.ensure_running:
        out = ensure_ibkr_running(cfg)
        print(json.dumps(out, sort_keys=True))
        return 0 if bool(out.get("ok")) else 2
    if args.health:
        out = ibkr_health(cfg)
        print(json.dumps(out, sort_keys=True))
        return 0 if bool(out.get("healthy")) else 2

    raise SystemExit("choose --ensure-running or --health")


if __name__ == "__main__":
    raise SystemExit(main())
