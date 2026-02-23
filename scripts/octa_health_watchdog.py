#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _append(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = dict(payload)
    row.setdefault("ts", _utc())
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")


def _probe(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


def _probe_x11(display: str) -> tuple[bool, str]:
    if not display:
        return False, "missing_display"
    if subprocess.run(["/usr/bin/env", "bash", "-lc", "command -v xdpyinfo >/dev/null 2>&1"], check=False).returncode == 0:
        ok = subprocess.run(["xdpyinfo", "-display", str(display)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False).returncode == 0
        return bool(ok), "xdpyinfo"
    if subprocess.run(["/usr/bin/env", "bash", "-lc", "command -v xset >/dev/null 2>&1"], check=False).returncode == 0:
        ok = subprocess.run(["xset", "-display", str(display), "-q"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False).returncode == 0
        return bool(ok), "xset"
    return True, "display_only"


def main() -> int:
    ap = argparse.ArgumentParser(description="Lightweight OCTA health watchdog")
    ap.add_argument("--events", default="octa/var/evidence/watchdog.jsonl")
    ap.add_argument("--interval-sec", type=float, default=5.0)
    ap.add_argument("--require-port", action="store_true", default=False)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7497)
    args = ap.parse_args()

    events = Path(args.events)
    while True:
        display = os.environ.get("DISPLAY", "")
        x11_ok, x11_probe_method = _probe_x11(display)
        port_ok = _probe(str(args.host), int(args.port)) if bool(args.require_port) else True
        healthy = bool(x11_ok and port_ok)
        _append(
            events,
            {
                "event_type": "watchdog_tick",
                "display": display,
                "x11_ok": x11_ok,
                "x11_probe_method": x11_probe_method,
                "port_ok": port_ok,
                "healthy": healthy,
            },
        )
        if not healthy:
            _append(events, {"event_type": "watchdog_fail", "reason": "unhealthy"})
            return 2
        time.sleep(float(args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
