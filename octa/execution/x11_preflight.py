from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CODE_OK = 0
CODE_DISPLAY_NOT_SET = 10
CODE_X_SERVER_UNREACHABLE = 11
CODE_XAUTHORITY_INVALID = 12
CODE_TOOL_MISSING = 13

REASON_OK = "OK"
REASON_DISPLAY_NOT_SET = "DISPLAY_NOT_SET"
REASON_X_SERVER_UNREACHABLE = "X_SERVER_UNREACHABLE"
REASON_XAUTHORITY_INVALID = "XAUTHORITY_INVALID"
REASON_TOOL_MISSING = "TOOL_MISSING"


@dataclass(frozen=True)
class _ProbeResult:
    ok: bool
    code: int
    reason: str
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": bool(self.ok),
            "code": int(self.code),
            "reason": str(self.reason),
            "details": dict(self.details),
        }


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _redact_env(env: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key in sorted(env.keys()):
        val = str(env[key])
        upper = key.upper()
        if any(tok in upper for tok in ("PASS", "SECRET", "TOKEN", "KEY", "USER")):
            out[key] = "<REDACTED>"
        else:
            out[key] = val
    return out


def check_display_env(*, env: dict[str, str] | None = None) -> dict[str, Any]:
    e = dict(os.environ if env is None else env)
    display = str(e.get("DISPLAY") or "").strip()
    if not display:
        return _ProbeResult(
            ok=False,
            code=CODE_DISPLAY_NOT_SET,
            reason=REASON_DISPLAY_NOT_SET,
            details={"display": "", "message": "DISPLAY is not set"},
        ).to_dict()
    return _ProbeResult(
        ok=True,
        code=CODE_OK,
        reason=REASON_OK,
        details={"display": display},
    ).to_dict()


def check_xauthority_access(*, env: dict[str, str] | None = None) -> dict[str, Any]:
    e = dict(os.environ if env is None else env)
    xauth_raw = str(e.get("XAUTHORITY") or "").strip()
    xauth_path = Path(xauth_raw).expanduser() if xauth_raw else Path.home() / ".Xauthority"
    exists = xauth_path.exists()
    readable = os.access(str(xauth_path), os.R_OK) if exists else False
    if not exists or not readable:
        return _ProbeResult(
            ok=False,
            code=CODE_XAUTHORITY_INVALID,
            reason=REASON_XAUTHORITY_INVALID,
            details={
                "xauthority": str(xauth_path),
                "exists": bool(exists),
                "readable": bool(readable),
                "message": "XAUTHORITY file is missing or not readable",
            },
        ).to_dict()
    return _ProbeResult(
        ok=True,
        code=CODE_OK,
        reason=REASON_OK,
        details={"xauthority": str(xauth_path), "exists": True, "readable": True},
    ).to_dict()


def _local_display_socket(display: str) -> Path | None:
    if not display.startswith(":"):
        return None
    suffix = display[1:].split(".", 1)[0].strip()
    if not suffix.isdigit():
        return None
    return Path("/tmp/.X11-unix") / f"X{suffix}"


def check_x_server_reachable(
    *,
    env: dict[str, str] | None = None,
    run_cmd: Any | None = None,
) -> dict[str, Any]:
    e = dict(os.environ if env is None else env)
    display = str(e.get("DISPLAY") or "").strip()
    if not display:
        return _ProbeResult(
            ok=False,
            code=CODE_DISPLAY_NOT_SET,
            reason=REASON_DISPLAY_NOT_SET,
            details={"display": "", "message": "DISPLAY is not set"},
        ).to_dict()

    run = run_cmd or subprocess.run
    probe_env = dict(e)
    probe_env["DISPLAY"] = display

    xset = shutil.which("xset")
    if xset:
        try:
            cp = run([xset, "q"], capture_output=True, text=True, check=False, timeout=3.0, env=probe_env)
            if int(cp.returncode) == 0:
                return _ProbeResult(
                    ok=True,
                    code=CODE_OK,
                    reason=REASON_OK,
                    details={"display": display, "probe": "xset q"},
                ).to_dict()
        except Exception as exc:
            return _ProbeResult(
                ok=False,
                code=CODE_X_SERVER_UNREACHABLE,
                reason=REASON_X_SERVER_UNREACHABLE,
                details={"display": display, "probe": "xset q", "error": f"{type(exc).__name__}:{exc}"},
            ).to_dict()

    xdpy = shutil.which("xdpyinfo")
    if xdpy:
        try:
            cp = run([xdpy, "-display", display], capture_output=True, text=True, check=False, timeout=3.0, env=probe_env)
            if int(cp.returncode) == 0:
                return _ProbeResult(
                    ok=True,
                    code=CODE_OK,
                    reason=REASON_OK,
                    details={"display": display, "probe": "xdpyinfo -display"},
                ).to_dict()
            stderr_tail = (cp.stderr or "").strip()[-500:]
            return _ProbeResult(
                ok=False,
                code=CODE_X_SERVER_UNREACHABLE,
                reason=REASON_X_SERVER_UNREACHABLE,
                details={"display": display, "probe": "xdpyinfo -display", "stderr_tail": stderr_tail},
            ).to_dict()
        except Exception as exc:
            return _ProbeResult(
                ok=False,
                code=CODE_X_SERVER_UNREACHABLE,
                reason=REASON_X_SERVER_UNREACHABLE,
                details={"display": display, "probe": "xdpyinfo -display", "error": f"{type(exc).__name__}:{exc}"},
            ).to_dict()

    sock = _local_display_socket(display)
    if sock is not None and sock.exists():
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect(str(sock))
            s.close()
            return _ProbeResult(
                ok=True,
                code=CODE_OK,
                reason=REASON_OK,
                details={"display": display, "probe": "unix_socket", "socket": str(sock)},
            ).to_dict()
        except Exception as exc:
            return _ProbeResult(
                ok=False,
                code=CODE_X_SERVER_UNREACHABLE,
                reason=REASON_X_SERVER_UNREACHABLE,
                details={"display": display, "probe": "unix_socket", "socket": str(sock), "error": f"{type(exc).__name__}:{exc}"},
            ).to_dict()

    return _ProbeResult(
        ok=False,
        code=CODE_X_SERVER_UNREACHABLE,
        reason=REASON_X_SERVER_UNREACHABLE,
        details={
            "display": display,
            "message": "No usable X11 probe available or X server unreachable",
            "xset_available": bool(xset),
            "xdpyinfo_available": bool(xdpy),
        },
    ).to_dict()


def check_required_tools(*, tool_lookup: Any | None = None) -> dict[str, Any]:
    lookup = tool_lookup or shutil.which
    required = ["xdotool", "wmctrl"]
    missing: list[str] = []
    resolved: dict[str, str] = {}
    for tool in required:
        path = lookup(tool)
        if path:
            resolved[tool] = str(path)
        else:
            missing.append(tool)
    if missing:
        return _ProbeResult(
            ok=False,
            code=CODE_TOOL_MISSING,
            reason=REASON_TOOL_MISSING,
            details={"missing_tools": sorted(missing), "resolved_tools": resolved},
        ).to_dict()
    return _ProbeResult(
        ok=True,
        code=CODE_OK,
        reason=REASON_OK,
        details={"resolved_tools": resolved},
    ).to_dict()


def _actions_for_reason(reason: str) -> list[str]:
    if reason == REASON_DISPLAY_NOT_SET:
        return [
            "Run from a real desktop session.",
            "Or export DISPLAY=:0.",
            "Or use --use-xvfb mode.",
        ]
    if reason == REASON_X_SERVER_UNREACHABLE:
        return [
            "Verify X server is running and reachable from this shell.",
            "Check DISPLAY points to a valid session.",
            "Run xset q or xdpyinfo manually to confirm.",
        ]
    if reason == REASON_XAUTHORITY_INVALID:
        return [
            "Set XAUTHORITY to a readable file for the active session.",
            "Verify file ownership/permissions.",
            "Retry preflight from the same desktop user context.",
        ]
    if reason == REASON_TOOL_MISSING:
        return [
            "Install missing tools: xdotool and wmctrl.",
            "Verify both are on PATH.",
        ]
    return ["No action required."]


def _write_sha_manifest(evidence_dir: Path) -> None:
    rows: list[str] = []
    for p in sorted(evidence_dir.rglob("*")):
        if not p.is_file():
            continue
        if p.name == "sha256sum.txt":
            continue
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        rows.append(f"{digest}  {p.relative_to(evidence_dir).as_posix()}")
    _write_text(evidence_dir / "sha256sum.txt", "\n".join(rows) + ("\n" if rows else ""))


def run_full_preflight(*, evidence_root: Path | None = None, env: dict[str, str] | None = None) -> dict[str, Any]:
    e = dict(os.environ if env is None else env)
    stamp = _utc_stamp()
    base = evidence_root or (Path("octa") / "var" / "evidence")
    evidence_dir = base / f"x11_preflight_{stamp}"
    evidence_dir.mkdir(parents=True, exist_ok=True)

    commands = [
        "check_display_env",
        "check_xauthority_access",
        "check_x_server_reachable",
        "check_required_tools",
    ]

    steps: list[dict[str, Any]] = []
    result = check_display_env(env=e)
    steps.append({"step": "check_display_env", **result})
    if result["ok"]:
        result = check_xauthority_access(env=e)
        steps.append({"step": "check_xauthority_access", **result})
    if result["ok"]:
        result = check_x_server_reachable(env=e)
        steps.append({"step": "check_x_server_reachable", **result})
    if result["ok"]:
        result = check_required_tools()
        steps.append({"step": "check_required_tools", **result})

    final = dict(result)
    final["ts_utc"] = _utc_iso()
    final["steps"] = steps
    final["actions"] = _actions_for_reason(str(final.get("reason", REASON_OK)))
    final["evidence_dir"] = str(evidence_dir)

    _write_json(evidence_dir / "result.json", final)
    _write_text(evidence_dir / "commands.txt", "\n".join(commands) + "\n")
    env_payload = {
        "DISPLAY": str(e.get("DISPLAY") or ""),
        "XAUTHORITY": str(e.get("XAUTHORITY") or ""),
        "PATH": str(e.get("PATH") or ""),
    }
    _write_json(evidence_dir / "environment.txt", _redact_env(env_payload))
    report_lines = [
        "# X11 Preflight Report",
        "",
        f"- timestamp_utc: `{final['ts_utc']}`",
        f"- ok: `{bool(final.get('ok'))}`",
        f"- code: `{int(final.get('code', CODE_X_SERVER_UNREACHABLE))}`",
        f"- reason: `{str(final.get('reason'))}`",
        "",
        "## Actions",
    ] + [f"- {a}" for a in final["actions"]]
    _write_text(evidence_dir / "report.md", "\n".join(report_lines) + "\n")
    _write_sha_manifest(evidence_dir)
    return final


if __name__ == "__main__":
    out = run_full_preflight()
    print(json.dumps(out, sort_keys=True))
    raise SystemExit(int(out.get("code", CODE_X_SERVER_UNREACHABLE)))
