#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from octa.execution.x11_preflight import run_full_preflight


EXIT_OK = 0
EXIT_WINDOW_NOT_FOUND = 21
EXIT_MISSING_CREDENTIALS = 22
EXIT_UNKNOWN_POPUP = 23
EXIT_LAUNCH_FAILED = 24
EXIT_LOGIN_FAILED = 25
EXIT_CONFIG_INVALID = 26


@dataclass(frozen=True)
class ChainConfig:
    launch_cmd: list[str]
    launch_cwd: Path
    launch_env: dict[str, str]
    main_title_contains: str
    login_title_contains: str
    user_env_name: str
    pass_env_name: str
    tab_to_username: int
    tab_to_password: int
    submit_key: str
    popup_whitelist: list[str]
    popup_action_map: dict[str, str]
    popup_timeout_sec: int


def _utc_stamp() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True) + "\n")


def _sha_manifest(evidence_dir: Path) -> None:
    rows: list[str] = []
    for p in sorted(evidence_dir.rglob("*")):
        if not p.is_file() or p.name == "sha256sum.txt":
            continue
        rows.append(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(evidence_dir).as_posix()}")
    _write_text(evidence_dir / "sha256sum.txt", "\n".join(rows) + ("\n" if rows else ""))


def _new_evidence_dir(base: Path, prefix: str) -> Path:
    stamp = _utc_stamp()
    candidate = base / f"{prefix}_{stamp}"
    if not candidate.exists():
        return candidate
    idx = 1
    while True:
        alt = base / f"{prefix}_{stamp}_{idx:02d}"
        if not alt.exists():
            return alt
        idx += 1


def _run(cmd: list[str], *, timeout: float = 10.0, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout, env=env)


def _screenshot(root_env: dict[str, str], out_path: Path, commands_log: list[str]) -> None:
    commands_log.append(f"import -window root {out_path}")
    cp = _run(["import", "-window", "root", str(out_path)], timeout=8.0, env=root_env)
    if cp.returncode != 0:
        _write_text(out_path.with_suffix(".txt"), f"screenshot_failed rc={cp.returncode}\n{cp.stderr or ''}")


def _wmctrl_windows(root_env: dict[str, str], commands_log: list[str]) -> tuple[int, str]:
    commands_log.append("wmctrl -lp")
    cp = _run(["wmctrl", "-lp"], timeout=5.0, env=root_env)
    return int(cp.returncode), cp.stdout or ""


def _parse_windows(raw: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for ln in raw.splitlines():
        line = ln.strip()
        if not line:
            continue
        parts = line.split(None, 4)
        if len(parts) < 5:
            continue
        out.append(
            {
                "wid": parts[0],
                "desktop": parts[1],
                "pid": parts[2],
                "host": parts[3],
                "title": parts[4],
            }
        )
    return out


def _find_window(windows: list[dict[str, str]], title_substr: str) -> dict[str, str] | None:
    want = str(title_substr).lower()
    for w in windows:
        if want in str(w.get("title", "")).lower():
            return w
    return None


def _focus_window(wid: str, root_env: dict[str, str], commands_log: list[str]) -> bool:
    commands_log.append(f"xdotool windowactivate {wid}")
    cp = _run(["xdotool", "windowactivate", str(wid)], timeout=4.0, env=root_env)
    return cp.returncode == 0


def _type_text(text: str, root_env: dict[str, str], commands_log: list[str]) -> bool:
    commands_log.append("xdotool type [REDACTED]")
    cp = _run(["xdotool", "type", "--delay", "30", text], timeout=8.0, env=root_env)
    return cp.returncode == 0


def _key(key: str, root_env: dict[str, str], commands_log: list[str]) -> bool:
    commands_log.append(f"xdotool key {key}")
    cp = _run(["xdotool", "key", key], timeout=4.0, env=root_env)
    return cp.returncode == 0


def _load_config(path: Path) -> ChainConfig:
    if not path.exists():
        raise ValueError(f"config_not_found:{path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("config_invalid_root")
    tws = data.get("tws")
    if not isinstance(tws, dict):
        raise ValueError("config_missing_tws")
    launch = tws.get("launch") if isinstance(tws.get("launch"), dict) else {}
    window = tws.get("window") if isinstance(tws.get("window"), dict) else {}
    login = tws.get("login") if isinstance(tws.get("login"), dict) else {}
    popups = tws.get("popups") if isinstance(tws.get("popups"), dict) else {}

    cmd = [str(x) for x in launch.get("command", [])] if isinstance(launch.get("command"), list) else []
    if not cmd:
        raise ValueError("config_missing_tws_launch_command")
    exe = Path(cmd[0]).expanduser()
    if not exe.exists() or not os.access(str(exe), os.X_OK):
        raise ValueError(f"launch_executable_invalid:{exe}")
    cmd[0] = str(exe)
    cwd = Path(str(launch.get("cwd") or Path.cwd())).expanduser()
    env_map = {str(k): str(v) for k, v in (launch.get("env") or {}).items()} if isinstance(launch.get("env"), dict) else {}

    main_title = str(window.get("main_title_contains") or "Trader Workstation")
    login_title = str(window.get("login_title_contains") or "Login")
    popup_whitelist = [str(x) for x in (popups.get("whitelist_titles") or []) if str(x).strip()]
    popup_action_map = {str(k): str(v) for k, v in (popups.get("action_map") or {}).items()} if isinstance(popups.get("action_map"), dict) else {}
    if not popup_whitelist:
        popup_whitelist = ["Disclaimer", "Important", "Agreement", "API Connection"]

    return ChainConfig(
        launch_cmd=cmd,
        launch_cwd=cwd,
        launch_env=env_map,
        main_title_contains=main_title,
        login_title_contains=login_title,
        user_env_name=str(login.get("username_env") or "OCTA_IBKR_USERNAME"),
        pass_env_name=str(login.get("password_env") or "OCTA_IBKR_PASSWORD"),
        tab_to_username=int(login.get("tab_to_username", 0)),
        tab_to_password=int(login.get("tab_to_password", 1)),
        submit_key=str(login.get("submit_key") or "Return"),
        popup_whitelist=popup_whitelist,
        popup_action_map=popup_action_map,
        popup_timeout_sec=int(popups.get("timeout_sec", 45)),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="TWS X11 autologin chain with deterministic preflight and popup whitelist.")
    ap.add_argument("--preflight-only", action="store_true", default=False)
    ap.add_argument("--config", default="configs/execution_ibkr.yaml")
    ap.add_argument("--timeout-sec", type=int, default=180)
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    evidence_dir = _new_evidence_dir(repo_root / "octa" / "var" / "evidence", "tws_x11_autologin")
    shots_dir = evidence_dir / "screenshots"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    shots_dir.mkdir(parents=True, exist_ok=True)
    commands_log: list[str] = []
    ui_events = evidence_dir / "ui_events.jsonl"
    preflight = run_full_preflight(evidence_root=repo_root / "octa" / "var" / "evidence")
    if not bool(preflight.get("ok")):
        reason = str(preflight.get("reason", "X_SERVER_UNREACHABLE"))
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": int(preflight.get("code", 11)),
            "reason": reason,
            "details": {"phase": "preflight", "x11_preflight_evidence": preflight.get("evidence_dir", "")},
        }
        _write_json(evidence_dir / "health.json", health)
        _write_text(evidence_dir / "commands.txt", "")
        _write_text(evidence_dir / "wmctrl_windows.txt", "")
        _write_text(
            evidence_dir / "report.md",
            "# TWS X11 Autologin Report\n\n- status: FAIL\n- reason: X11_PREFLIGHT_FAILED\n"
            f"- x11_preflight_evidence: {preflight.get('evidence_dir', '')}\n",
        )
        _write_text(ui_events, "")
        _sha_manifest(evidence_dir)
        print(f"❌ X11 preflight failed: {reason}")
        print("Action:")
        for action in preflight.get("actions", []):
            print(f"  - {action}")
        print(
            json.dumps(
                {"x11_preflight_evidence": preflight.get("evidence_dir", ""), "evidence_dir": str(evidence_dir)},
                sort_keys=True,
            )
        )
        return int(preflight.get("code", 11))

    if args.preflight_only:
        _write_json(
            evidence_dir / "health.json",
            {"ts_utc": _utc_iso(), "ok": True, "code": EXIT_OK, "reason": "PREFLIGHT_ONLY_OK", "details": {"x11_preflight_evidence": preflight.get("evidence_dir", "")}},
        )
        _write_text(evidence_dir / "commands.txt", "")
        _write_text(evidence_dir / "wmctrl_windows.txt", "")
        _write_text(evidence_dir / "report.md", "# TWS X11 Autologin Report\n\n- status: PASS\n- reason: PREFLIGHT_ONLY_OK\n")
        _write_text(ui_events, "")
        _sha_manifest(evidence_dir)
        print(json.dumps({"status": "x11_preflight_ok", "x11_preflight_evidence": preflight.get("evidence_dir", ""), "evidence_dir": str(evidence_dir)}, sort_keys=True))
        return EXIT_OK

    try:
        cfg = _load_config(Path(args.config))
    except Exception as exc:
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": EXIT_CONFIG_INVALID,
            "reason": "CONFIG_INVALID",
            "details": {"error": f"{type(exc).__name__}:{exc}"},
        }
        _write_json(evidence_dir / "health.json", health)
        _write_text(evidence_dir / "commands.txt", "")
        _write_text(evidence_dir / "wmctrl_windows.txt", "")
        _write_text(evidence_dir / "report.md", "# TWS X11 Autologin Report\n\n- status: FAIL\n- reason: CONFIG_INVALID\n")
        _sha_manifest(evidence_dir)
        print(json.dumps({"evidence_dir": str(evidence_dir), "code": EXIT_CONFIG_INVALID, "reason": "CONFIG_INVALID"}, sort_keys=True))
        return EXIT_CONFIG_INVALID

    root_env = dict(os.environ)
    for k, v in cfg.launch_env.items():
        root_env[k] = v

    user = str(root_env.get(cfg.user_env_name) or "")
    pw = str(root_env.get(cfg.pass_env_name) or "")
    if not user or not pw:
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": EXIT_MISSING_CREDENTIALS,
            "reason": "MISSING_CREDENTIALS",
            "details": {"username_env": cfg.user_env_name, "password_env": cfg.pass_env_name},
        }
        _write_json(evidence_dir / "health.json", health)
        _write_text(evidence_dir / "commands.txt", "")
        _write_text(evidence_dir / "wmctrl_windows.txt", "")
        _write_text(evidence_dir / "report.md", "# TWS X11 Autologin Report\n\n- status: FAIL\n- reason: MISSING_CREDENTIALS\n")
        _sha_manifest(evidence_dir)
        print(json.dumps({"evidence_dir": str(evidence_dir), "code": EXIT_MISSING_CREDENTIALS, "reason": "MISSING_CREDENTIALS"}, sort_keys=True))
        return EXIT_MISSING_CREDENTIALS

    # Launch TWS
    commands_log.append(" ".join(cfg.launch_cmd))
    log_path = repo_root / "logs" / "ibkr_broker.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with log_path.open("a", encoding="utf-8") as logf:
            proc = subprocess.Popen(  # noqa: S603
                cfg.launch_cmd,
                cwd=str(cfg.launch_cwd),
                env=root_env,
                stdout=logf,
                stderr=logf,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
    except Exception as exc:
        _screenshot(root_env, shots_dir / "launch_failed_root.png", commands_log)
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": EXIT_LAUNCH_FAILED,
            "reason": "LAUNCH_FAILED",
            "details": {"error": f"{type(exc).__name__}:{exc}", "command": cfg.launch_cmd},
        }
        _write_json(evidence_dir / "health.json", health)
        _write_text(evidence_dir / "commands.txt", "\n".join(commands_log) + "\n")
        _write_text(evidence_dir / "wmctrl_windows.txt", "")
        _write_text(evidence_dir / "report.md", "# TWS X11 Autologin Report\n\n- status: FAIL\n- reason: LAUNCH_FAILED\n")
        _sha_manifest(evidence_dir)
        print(json.dumps({"evidence_dir": str(evidence_dir), "code": EXIT_LAUNCH_FAILED, "reason": "LAUNCH_FAILED"}, sort_keys=True))
        return EXIT_LAUNCH_FAILED

    # Wait for main/login window
    deadline = time.monotonic() + max(10, int(args.timeout_sec))
    login_win: dict[str, str] | None = None
    main_win: dict[str, str] | None = None
    last_windows_txt = ""
    while time.monotonic() < deadline:
        rc, out = _wmctrl_windows(root_env, commands_log)
        if rc == 0:
            last_windows_txt = out
            wins = _parse_windows(out)
            login_win = _find_window(wins, cfg.login_title_contains)
            main_win = _find_window(wins, cfg.main_title_contains)
            if login_win or main_win:
                break
        time.sleep(0.5)

    _write_text(evidence_dir / "wmctrl_windows.txt", last_windows_txt)
    _screenshot(root_env, shots_dir / "before_login.png", commands_log)

    if not login_win and not main_win:
        _screenshot(root_env, shots_dir / "window_not_found_root.png", commands_log)
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": EXIT_WINDOW_NOT_FOUND,
            "reason": "WINDOW_NOT_FOUND",
            "details": {
                "login_title_contains": cfg.login_title_contains,
                "main_title_contains": cfg.main_title_contains,
            },
        }
        _write_json(evidence_dir / "health.json", health)
        _write_text(evidence_dir / "commands.txt", "\n".join(commands_log) + "\n")
        _write_text(
            evidence_dir / "report.md",
            "# TWS X11 Autologin Report\n\n- status: FAIL\n- reason: WINDOW_NOT_FOUND\n- action: update tws.window.* title substrings from wmctrl_windows.txt\n",
        )
        _sha_manifest(evidence_dir)
        print(json.dumps({"evidence_dir": str(evidence_dir), "code": EXIT_WINDOW_NOT_FOUND, "reason": "WINDOW_NOT_FOUND"}, sort_keys=True))
        return EXIT_WINDOW_NOT_FOUND

    # Login (if login window present)
    if login_win:
        _focus_window(login_win["wid"], root_env, commands_log)
        for _ in range(max(0, int(cfg.tab_to_username))):
            _key("Tab", root_env, commands_log)
        _type_text(user, root_env, commands_log)
        for _ in range(max(0, int(cfg.tab_to_password))):
            _key("Tab", root_env, commands_log)
        _type_text(pw, root_env, commands_log)
        _key(cfg.submit_key, root_env, commands_log)
        # No post-typing screenshot to avoid capturing password field content.

    # Popup/disclaimer handling
    popup_deadline = time.monotonic() + max(1, int(cfg.popup_timeout_sec))
    unknown_popup: dict[str, str] | None = None
    while time.monotonic() < popup_deadline:
        rc, out = _wmctrl_windows(root_env, commands_log)
        if rc != 0:
            time.sleep(0.5)
            continue
        wins = _parse_windows(out)
        main_win = _find_window(wins, cfg.main_title_contains) or main_win
        handled_any = False
        for w in wins:
            title = str(w.get("title", ""))
            if not title:
                continue
            matched = None
            for token in cfg.popup_whitelist:
                if token.lower() in title.lower():
                    matched = token
                    break
            if matched is None:
                # treat only non-main/login extras as unknown popup candidates
                if cfg.main_title_contains.lower() in title.lower():
                    continue
                if cfg.login_title_contains.lower() in title.lower():
                    continue
                unknown_popup = w
                break
            action_key = cfg.popup_action_map.get(matched, "Return")
            ok_focus = _focus_window(w["wid"], root_env, commands_log)
            ok_action = _key(action_key, root_env, commands_log) if ok_focus else False
            handled_any = True
            _append_jsonl(
                ui_events,
                {
                    "ts_utc": _utc_iso(),
                    "title": title,
                    "wid": w["wid"],
                    "action": action_key,
                    "success": bool(ok_focus and ok_action),
                },
            )
        if unknown_popup is not None:
            break
        if main_win and not handled_any:
            break
        time.sleep(0.5)

    if unknown_popup is not None:
        _screenshot(root_env, shots_dir / "unknown_popup.png", commands_log)
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": EXIT_UNKNOWN_POPUP,
            "reason": "UNKNOWN_POPUP",
            "details": {"title": unknown_popup.get("title", ""), "wid": unknown_popup.get("wid", "")},
        }
        _write_json(evidence_dir / "health.json", health)
        _write_text(evidence_dir / "commands.txt", "\n".join(commands_log) + "\n")
        _write_text(
            evidence_dir / "report.md",
            "# TWS X11 Autologin Report\n\n- status: FAIL\n- reason: UNKNOWN_POPUP\n- action: add title token to tws.popups.whitelist_titles and map action in tws.popups.action_map\n",
        )
        _sha_manifest(evidence_dir)
        print(json.dumps({"evidence_dir": str(evidence_dir), "code": EXIT_UNKNOWN_POPUP, "reason": "UNKNOWN_POPUP"}, sort_keys=True))
        return EXIT_UNKNOWN_POPUP

    # Final success condition: main window present or login window no longer present
    rc, out = _wmctrl_windows(root_env, commands_log)
    wins = _parse_windows(out if rc == 0 else "")
    main_win = _find_window(wins, cfg.main_title_contains)
    login_win = _find_window(wins, cfg.login_title_contains)
    if not main_win and login_win:
        _screenshot(root_env, shots_dir / "login_failed.png", commands_log)
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": EXIT_LOGIN_FAILED,
            "reason": "LOGIN_NOT_CONFIRMED",
            "details": {"login_title_contains": cfg.login_title_contains, "main_title_contains": cfg.main_title_contains},
        }
        _write_json(evidence_dir / "health.json", health)
        _write_text(evidence_dir / "commands.txt", "\n".join(commands_log) + "\n")
        _write_text(
            evidence_dir / "report.md",
            "# TWS X11 Autologin Report\n\n- status: FAIL\n- reason: LOGIN_NOT_CONFIRMED\n- action: verify title substrings or login tab strategy in config\n",
        )
        _sha_manifest(evidence_dir)
        print(json.dumps({"evidence_dir": str(evidence_dir), "code": EXIT_LOGIN_FAILED, "reason": "LOGIN_NOT_CONFIRMED"}, sort_keys=True))
        return EXIT_LOGIN_FAILED

    _screenshot(root_env, shots_dir / "post_login_root.png", commands_log)
    health = {
        "ts_utc": _utc_iso(),
        "ok": True,
        "code": EXIT_OK,
        "reason": "OK",
        "details": {
            "tws_pid": int(proc.pid),
            "main_window_found": bool(main_win),
            "login_window_present": bool(login_win),
            "evidence_dir": str(evidence_dir),
        },
    }
    _write_json(evidence_dir / "health.json", health)
    _write_text(evidence_dir / "commands.txt", "\n".join(commands_log) + "\n")
    _write_text(
        evidence_dir / "report.md",
        "# TWS X11 Autologin Report\n\n- status: PASS\n- reason: OK\n",
    )
    if not ui_events.exists():
        _write_text(ui_events, "")
    _sha_manifest(evidence_dir)
    print(json.dumps({"evidence_dir": str(evidence_dir), "code": EXIT_OK, "reason": "OK"}, sort_keys=True))
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
