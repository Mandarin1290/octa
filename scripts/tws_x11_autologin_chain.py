#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
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

# Number of consecutive clean polls required before the confirm loop exits OK.
# A single clean poll is not sufficient because a popup may appear between
# the popup-drain phase and the final sweep.
_STABLE_OK_NEEDED = 3

STATE_CLASSIFY = "CLASSIFY"
STATE_ROUTE = "ROUTE"
STATE_ACT = "ACT"
STATE_CONFIRM = "CONFIRM"
STATE_STABILIZE = "STABILIZE"
STATE_RECHECK = "RECHECK"
STATE_DONE = "DONE"
STATE_FAIL = "FAIL"

WIN_MAIN = "MAIN"
WIN_LOGIN = "LOGIN"
WIN_POPUP_WARN = "POPUP_WARN"
WIN_POPUP_DISCLAIMER = "POPUP_DISCLAIMER"
WIN_POPUP_LOGIN_MESSAGES = "POPUP_LOGIN_MESSAGES"
WIN_TRANSIENT_CLOSING = "TRANSIENT_CLOSING"
WIN_OTHER = "OTHER"

BLOCKING_TOKENS = [
    "Börsenspiegel",
    "Boersenspiegel",
    "Warnhinweis",
    "Risikohinweis",
    "Disclaimer",
    "Login Messages",
    "Login Message",
    "Messages",
    "Login Messenger",
    "IBKR Login Messenger",
    "Messenger",
    "Programm wird geschlossen",
    "Programm wird geschlossen...",
    "win0",
    # Dow Jones news popups (German + English locale variants)
    "Dow Jones Heutige Top 10",
    "Dow Jones",
    "Heutige Top 10",
    "Top 10 Today",
]

_UI_EVENTS_PATH: Path | None = None
_UI_EVENTS_LIMIT = 1200


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
    api_host: str
    api_port: int


@dataclass(frozen=True)
class ActionRule:
    action: str
    retries: int = 1


@dataclass(frozen=True)
class WindowInfo:
    wid_hex: str
    wid_dec: int
    desktop: str
    title: str
    pid: str
    source: str = "wmctrl"
    matched_tokens: tuple[str, ...] = ()


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


def _set_ui_events_path(path: Path | None) -> None:
    global _UI_EVENTS_PATH
    _UI_EVENTS_PATH = path


def _log_ui_event(payload: dict[str, Any]) -> None:
    if _UI_EVENTS_PATH is None:
        return
    _append_jsonl(_UI_EVENTS_PATH, payload)


def _log_state_transition(prev_state: str, next_state: str, *, reason: str = "", target: str = "", wid: str = "") -> None:
    _log_ui_event(
        {
            "ts_utc": _utc_iso(),
            "kind": "state_transition",
            "state_from": str(prev_state),
            "state_to": str(next_state),
            "reason": str(reason),
            "target_title": str(target),
            "target_wid_hex": str(wid),
        }
    )


def _run_gui_cmd(
    cmd: list[str],
    *,
    timeout: float,
    env: dict[str, str],
    state: str,
    step: str,
    action: str = "",
    title: str = "",
    token: str = "",
    wid_wmctrl: str = "",
    wid_xdotool: str = "",
) -> subprocess.CompletedProcess[str]:
    cp = _run(cmd, timeout=timeout, env=env)
    if cmd and cmd[0] in {"wmctrl", "xdotool", "xwininfo"}:
        _log_ui_event(
            {
                "ts_utc": _utc_iso(),
                "kind": "command",
                "state": str(state),
                "action": str(action),
                "step": str(step),
                "title": str(title),
                "token": str(token),
                "wid_wmctrl": str(wid_wmctrl),
                "wid_hex": str(wid_wmctrl),
                "wid_xdotool": str(wid_xdotool),
                "cmd": " ".join(str(x) for x in cmd),
                "rc": int(cp.returncode),
                "stderr_tail": _tail(cp.stderr or "", 400),
            }
        )
    return cp


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
    cp = _run_gui_cmd(["wmctrl", "-lp"], timeout=5.0, env=root_env, state="GLOBAL", step="wmctrl_list")
    return int(cp.returncode), cp.stdout or ""


def _tail(text: str, n: int = 400) -> str:
    s = str(text or "")
    return s[-n:] if len(s) > n else s


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


def _parse_wmctrl_windows(raw: str) -> list[dict[str, str]]:
    return _parse_windows(raw)


def _find_window(windows: list[dict[str, str]], title_substr: str) -> dict[str, str] | None:
    want = str(title_substr).lower()
    for w in windows:
        if want in str(w.get("title", "")).lower():
            return w
    return None


def _find_window_any(windows: list[dict[str, str]], title_substrs: list[str]) -> dict[str, str] | None:
    for token in title_substrs:
        w = _find_window(windows, token)
        if w is not None:
            return w
    return None


def _main_detection_tokens(main_title_contains: str) -> list[str]:
    return [str(main_title_contains or ""), "Interactive Brokers", "IBKR", "(Simulated Trading)", "Trader Workstation", "DUH"]


def _find_main_windows(windows: list[dict[str, str]], main_title_contains: str) -> list[dict[str, str]]:
    toks = [t for t in _main_detection_tokens(main_title_contains) if str(t).strip()]
    out: list[dict[str, str]] = []
    for w in windows:
        title = str(w.get("title", ""))
        if not title:
            continue
        if any(tok.lower() in title.lower() for tok in toks):
            out.append(w)
    return out


def _match_title(windows: list[dict[str, str]], title_substr: str) -> dict[str, str] | None:
    return _find_window(windows, title_substr)


def _pick_action_rule(
    title: str, whitelist: list[str], action_map: dict[str, ActionRule]
) -> tuple[str | None, ActionRule]:
    default = ActionRule(action="Return", retries=1)
    title_l = str(title or "").lower()
    for token in whitelist:
        tok = str(token or "").strip()
        if not tok:
            continue
        if tok.lower() in title_l:
            return tok, action_map.get(tok, default)
    return None, default


def _recommended_offsets(geom: dict[str, int]) -> dict[str, dict[str, int]]:
    x = int(geom.get("x", 0))
    y = int(geom.get("y", 0))
    w = max(1, int(geom.get("width", 1)))
    h = max(1, int(geom.get("height", 1)))
    return {
        "user": {"x": x + max(1, int(w * 0.20)), "y": y + max(1, int(h * 0.35))},
        "pass": {"x": x + max(1, int(w * 0.20)), "y": y + max(1, int(h * 0.45))},
        "warnhinweis_button": {"x": x + max(1, int(w * 0.80)), "y": y + max(1, int(h * 0.90))},
    }


def _focus_window(wid: str, root_env: dict[str, str], commands_log: list[str]) -> bool:
    xwid = _wid_for_xdotool(wid)
    commands_log.append(f"xdotool windowactivate {xwid}")
    cp = _run_gui_cmd(
        ["xdotool", "windowactivate", str(xwid)],
        timeout=4.0,
        env=root_env,
        state="GLOBAL",
        step="xdotool_windowactivate",
        wid_wmctrl=str(wid),
        wid_xdotool=str(xwid),
    )
    return cp.returncode == 0


def _type_text(text: str, root_env: dict[str, str], commands_log: list[str]) -> bool:
    commands_log.append("xdotool type [REDACTED]")
    cp = _run_gui_cmd(["xdotool", "type", "--delay", "30", text], timeout=8.0, env=root_env, state="GLOBAL", step="xdotool_type")
    return cp.returncode == 0


def _key(key: str, root_env: dict[str, str], commands_log: list[str]) -> bool:
    commands_log.append(f"xdotool key {key}")
    cp = _run_gui_cmd(["xdotool", "key", key], timeout=4.0, env=root_env, state="GLOBAL", step="xdotool_key")
    return cp.returncode == 0


def _window_geometry(wid: str, root_env: dict[str, str], commands_log: list[str]) -> dict[str, int]:
    xwid = _wid_for_xdotool(wid)
    commands_log.append(f"xdotool getwindowgeometry --shell {xwid}")
    cp = _run_gui_cmd(
        ["xdotool", "getwindowgeometry", "--shell", str(xwid)],
        timeout=4.0,
        env=root_env,
        state="GLOBAL",
        step="xdotool_getwindowgeometry",
        wid_wmctrl=str(wid),
        wid_xdotool=str(xwid),
    )
    out: dict[str, int] = {}
    if cp.returncode != 0:
        return out
    for ln in (cp.stdout or "").splitlines():
        if "=" not in ln:
            continue
        k, v = ln.split("=", 1)
        key = str(k).strip().lower()
        val = str(v).strip()
        if key in {"x", "y", "width", "height"}:
            try:
                out[key] = int(val)
            except Exception:
                pass
    return out


def _click_absolute(x: int, y: int, root_env: dict[str, str], commands_log: list[str]) -> bool:
    commands_log.append(f"xdotool mousemove {x} {y}")
    cp_move = _run_gui_cmd(
        ["xdotool", "mousemove", str(int(x)), str(int(y))],
        timeout=4.0,
        env=root_env,
        state="GLOBAL",
        step="xdotool_mousemove_abs",
    )
    commands_log.append("xdotool click 1")
    cp_click = _run_gui_cmd(["xdotool", "click", "1"], timeout=4.0, env=root_env, state="GLOBAL", step="xdotool_click")
    return bool(cp_move.returncode == 0 and cp_click.returncode == 0)


def _click_focus_in_login(
    wid: str, root_env: dict[str, str], commands_log: list[str]
) -> tuple[bool, dict[str, Any]]:
    xwid = _wid_for_xdotool(wid)
    geom = _window_geometry(wid, root_env, commands_log)
    width = max(1, int(geom.get("width", 1)))
    height = max(1, int(geom.get("height", 1)))
    off_x = max(1, int(width * 0.20))
    off_y = max(1, int(height * 0.35))
    commands_log.append(f"xdotool mousemove --window {xwid} {off_x} {off_y}")
    cp_move = _run_gui_cmd(
        ["xdotool", "mousemove", "--window", str(xwid), str(off_x), str(off_y)],
        timeout=4.0,
        env=root_env,
        state="GLOBAL",
        step="xdotool_mousemove_window",
        wid_wmctrl=str(wid),
        wid_xdotool=str(xwid),
    )
    commands_log.append("xdotool click 1")
    cp_click = _run_gui_cmd(
        ["xdotool", "click", "1"], timeout=4.0, env=root_env, state="GLOBAL", step="xdotool_click", wid_wmctrl=str(wid), wid_xdotool=str(xwid)
    )
    meta = {
        "x": off_x,
        "y": off_y,
        "geometry": {"x": int(geom.get("x", 0)), "y": int(geom.get("y", 0)), "width": width, "height": height},
    }
    return bool(cp_move.returncode == 0 and cp_click.returncode == 0), meta


def _api_connectable(host: str, port: int) -> tuple[bool, str]:
    try:
        with socket.create_connection((str(host), int(port)), timeout=1.0):
            return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}:{exc}"


def _wid_for_xdotool(wid: str) -> str:
    s = str(wid or "").strip()
    if s.lower().startswith("0x"):
        try:
            return str(int(s, 16))
        except Exception:
            return s
    return s


def _wid_hex_norm(wid: str) -> str:
    s = str(wid or "").strip().lower()
    if not s:
        return ""
    try:
        if s.startswith("0x"):
            return hex(int(s, 16))
        return hex(int(s))
    except Exception:
        return s


def _is_window_still_present(root_env: dict[str, str], commands_log: list[str], wid_raw: str, title: str) -> bool:
    rc_chk, out_chk = _wmctrl_windows(root_env, commands_log)
    if rc_chk != 0:
        return True
    now = _parse_windows(out_chk)
    wid_s = str(wid_raw or "")
    title_l = str(title or "").lower()
    return any((wid_s and str(x.get("wid", "")) == wid_s) or (title_l and title_l in str(x.get("title", "")).lower()) for x in now)


def _confirm_window_closed(root_env: dict[str, str], commands_log: list[str], wid_raw: str, title: str, polls: int = 2) -> bool:
    gone = 0
    for _ in range(max(2, int(polls) * 2)):
        still_wm = _is_window_still_present(root_env, commands_log, wid_raw, title)
        still_xd = False
        tok = str(title or "").strip()
        if tok:
            xfound = _xdotool_find_windows_by_tokens(root_env, commands_log, [tok])
            still_xd = any(tok.lower() in str(x.get("title", "")).lower() for x in xfound)
        if still_wm or still_xd:
            gone = 0
        else:
            gone += 1
            if gone >= int(polls):
                return True
        time.sleep(0.2)
    return False


def _xdotool_find_windows_by_tokens(
    root_env: dict[str, str], commands_log: list[str], tokens: list[str]
) -> list[dict[str, str]]:
    found: dict[str, dict[str, str]] = {}
    for token in tokens:
        tok = str(token or "").strip()
        if not tok:
            continue
        commands_log.append(f"xdotool search --name {tok}")
        cp = _run_gui_cmd(["xdotool", "search", "--name", tok], timeout=4.0, env=root_env, state="GLOBAL", step="xdotool_search", token=tok)
        if cp.returncode != 0:
            continue
        for ln in (cp.stdout or "").splitlines():
            wid = str(ln or "").strip()
            if not wid:
                continue
            commands_log.append(f"xdotool getwindowname {wid}")
            cp_name = _run_gui_cmd(
                ["xdotool", "getwindowname", wid],
                timeout=4.0,
                env=root_env,
                state="GLOBAL",
                step="xdotool_getwindowname",
                token=tok,
                wid_xdotool=str(wid),
            )
            title = (cp_name.stdout or "").strip()
            if wid not in found:
                found[wid] = {"wid": wid, "title": title, "matched_token": tok}
    return list(found.values())


def _list_windows_wmctrl(root_env: dict[str, str], commands_log: list[str]) -> tuple[int, str, list[WindowInfo]]:
    rc, out = _wmctrl_windows(root_env, commands_log)
    if rc != 0:
        return rc, out, []
    wins: list[WindowInfo] = []
    for w in _parse_windows(out):
        wid_hex = str(w.get("wid", "")).strip()
        if not wid_hex:
            continue
        try:
            wid_dec = int(_wid_for_xdotool(wid_hex))
        except Exception:
            wid_dec = -1
        wins.append(
            WindowInfo(
                wid_hex=wid_hex,
                wid_dec=wid_dec,
                desktop=str(w.get("desktop", "")),
                title=str(w.get("title", "")),
                pid=str(w.get("pid", ""),),
                source="wmctrl",
            )
        )
    return rc, out, wins


def _classify_window(window: WindowInfo, cfg: ChainConfig, main_tokens: list[str]) -> str:
    title = str(window.title or "")
    t = title.lower()
    if t.startswith("duh"):
        return WIN_MAIN
    if any(str(tok).lower() in t for tok in main_tokens if str(tok).strip()):
        return WIN_MAIN
    if str(cfg.login_title_contains).lower() in t:
        return WIN_LOGIN
    if "warnhinweis" in t:
        return WIN_POPUP_WARN
    if "disclaimer" in t:
        return WIN_POPUP_DISCLAIMER
    if "login messages" in t or "login message" in t or "login messenger" in t or t == "messages" or "messages" in t or " messenger" in t:
        return WIN_POPUP_LOGIN_MESSAGES
    if "programm wird geschlossen" in t or t == "win0":
        return WIN_TRANSIENT_CLOSING
    return WIN_OTHER


def _wmctrl_activate(hex_wid: str, root_env: dict[str, str], commands_log: list[str]) -> subprocess.CompletedProcess[str]:
    commands_log.append(f"wmctrl -ia {hex_wid}")
    return _run_gui_cmd(["wmctrl", "-ia", str(hex_wid)], timeout=4.0, env=root_env, state=STATE_ACT, step="wmctrl_activate", wid_wmctrl=str(hex_wid))


def _wmctrl_close(hex_wid: str, root_env: dict[str, str], commands_log: list[str]) -> subprocess.CompletedProcess[str]:
    commands_log.append(f"wmctrl -ic {hex_wid}")
    return _run_gui_cmd(["wmctrl", "-ic", str(hex_wid)], timeout=4.0, env=root_env, state=STATE_ACT, step="wmctrl_ic", wid_wmctrl=str(hex_wid))


def _resolve_xdotool_wid(
    root_env: dict[str, str], commands_log: list[str], title: str, matched_tokens: tuple[str, ...]
) -> str:
    probes: list[str] = []
    if str(title).strip():
        probes.append(str(title).strip())
    for tok in matched_tokens:
        t = str(tok).strip()
        if t and t not in probes:
            probes.append(t)
    for p in probes:
        commands_log.append(f"xdotool search --name {p}")
        cp = _run_gui_cmd(["xdotool", "search", "--name", p], timeout=4.0, env=root_env, state=STATE_ACT, step="xdotool_search", title=title, token=p)
        if cp.returncode != 0:
            continue
        for ln in (cp.stdout or "").splitlines():
            wid = str(ln or "").strip()
            if wid:
                return wid
    return ""


def _first_matching_token(title: str, tokens: list[str]) -> str:
    t = str(title or "").lower()
    for token in tokens:
        tok = str(token or "").strip()
        if tok and tok.lower() in t:
            return tok
    return ""


def _xwininfo_geometry(wid_hex: str, root_env: dict[str, str], commands_log: list[str]) -> dict[str, int]:
    commands_log.append(f"xwininfo -id {wid_hex}")
    cp = _run_gui_cmd(["xwininfo", "-id", str(wid_hex)], timeout=4.0, env=root_env, state=STATE_ACT, step="xwininfo_geometry", wid_wmctrl=str(wid_hex))
    if cp.returncode != 0:
        return {}
    geom: dict[str, int] = {}
    for ln in (cp.stdout or "").splitlines():
        line = str(ln or "").strip()
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        key = k.strip().lower()
        val_s = v.strip().split()[0] if v.strip() else ""
        try:
            val = int(val_s)
        except Exception:
            continue
        if key == "absolute upper-left x":
            geom["x"] = val
        elif key == "absolute upper-left y":
            geom["y"] = val
        elif key == "width":
            geom["width"] = val
        elif key == "height":
            geom["height"] = val
    return geom


def _abs_click(x: int, y: int, root_env: dict[str, str], commands_log: list[str]) -> bool:
    return _click_absolute(x, y, root_env, commands_log)


def _confirm_absent(
    root_env: dict[str, str], commands_log: list[str], tokens: list[str], polls: int = 2, tws_pid: int | None = None
) -> tuple[bool, int]:
    need = max(2, int(polls))
    absent = 0
    for _ in range(max(10, need * 5)):
        rc, out = _wmctrl_windows(root_env, commands_log)
        if rc != 0:
            absent = 0
            time.sleep(0.25)
            continue
        wins = _parse_windows(out)
        present = False
        for w in wins:
            title = str(w.get("title", "")).lower()
            if not any(str(tok or "").strip().lower() in title for tok in tokens if str(tok or "").strip()):
                continue
            if tws_pid is not None and not _pid_matches_tws_or_zero(str(w.get("pid", "")), int(tws_pid)):
                continue
            if any(str(tok or "").strip().lower() in title for tok in tokens if str(tok or "").strip()):
                present = True
                break
        if present:
            absent = 0
        else:
            absent += 1
            if absent >= need:
                return True, absent
        time.sleep(0.25)
    return False, absent


def _matches_any_token(title: str, tokens: list[str]) -> bool:
    tl = str(title or "").lower()
    return any(str(tok or "").strip().lower() in tl for tok in tokens if str(tok or "").strip())


def _pid_matches_tws_or_zero(pid_raw: str, tws_pid: int) -> bool:
    p = str(pid_raw or "").strip()
    return p == str(int(tws_pid)) or p == "0"


def _collect_blocking_popups_for_pid(
    root_env: dict[str, str], commands_log: list[str], tokens: list[str], tws_pid: int
) -> list[dict[str, str]]:
    rc, out = _wmctrl_windows(root_env, commands_log)
    if rc != 0:
        return []
    found: list[dict[str, str]] = []
    for w in _parse_windows(out):
        title = str(w.get("title", "")).strip()
        if not title:
            continue
        if not _matches_any_token(title, tokens):
            continue
        pid = str(w.get("pid", "")).strip()
        if not _pid_matches_tws_or_zero(pid, tws_pid):
            continue
        found.append(
            {
                "wid_hex": str(w.get("wid", "")).strip(),
                "desktop": str(w.get("desktop", "")).strip(),
                "pid": pid,
                "title": title,
                "source": "wmctrl",
            }
        )
    return found


def _final_blocking_sweep(
    root_env: dict[str, str], commands_log: list[str], tokens: list[str], tws_pid: int
) -> list[dict[str, str]]:
    found = _collect_blocking_popups_for_pid(root_env, commands_log, tokens, tws_pid)
    if found:
        return found
    xwins = _xdotool_find_windows_by_tokens(root_env, commands_log, tokens)
    fallback: list[dict[str, str]] = []
    for xw in xwins:
        title = str(xw.get("title", "")).strip()
        if not title or not _matches_any_token(title, tokens):
            continue
        fallback.append(
            {
                "wid_hex": str(xw.get("wid", "")).strip(),
                "desktop": "",
                "pid": "",
                "title": title,
                "source": "xdotool_fallback",
            }
        )
    return fallback


def _step_event(
    ui_events: Path,
    *,
    state: str,
    action: str,
    step: str,
    title: str,
    token: str,
    wid_wmctrl: str,
    wid_xdotool: str,
    cmd: str,
    rc: int,
    stderr_tail: str = "",
    click_x: int | None = None,
    click_y: int | None = None,
    still_present_after: bool | None = None,
    confirm_result: str = "",
    confirm_streak: int | None = 0,
) -> None:
    evt: dict[str, Any] = {
        "ts_utc": _utc_iso(),
        "state": state,
        "action": action,
        "step": step,
        "title": title,
        "token": token,
        "wid_wmctrl": wid_wmctrl,
        "wid_hex": wid_wmctrl,
        "wid_xdotool": wid_xdotool,
        "cmd": cmd,
        "command": cmd,
        "rc": int(rc),
        "stderr_tail": stderr_tail,
    }
    if click_x is not None:
        evt["click_x"] = int(click_x)
    if click_y is not None:
        evt["click_y"] = int(click_y)
    if still_present_after is not None:
        evt["still_present_after"] = bool(still_present_after)
    if confirm_result:
        evt["confirm_result"] = str(confirm_result)
    evt["confirm_streak"] = int(confirm_streak or 0)
    _append_jsonl(ui_events, evt)


def _blocking_popup_titles_dual(root_env: dict[str, str], commands_log: list[str], tokens: list[str]) -> list[str]:
    titles: list[str] = []
    rc, out = _wmctrl_windows(root_env, commands_log)
    if rc == 0:
        for w in _parse_windows(out):
            t = str(w.get("title", "")).strip()
            if t and any(tok.lower() in t.lower() for tok in tokens):
                titles.append(t)
    xwins = _xdotool_find_windows_by_tokens(root_env, commands_log, tokens)
    for xw in xwins:
        t = str(xw.get("title", "")).strip()
        if t and t not in titles:
            titles.append(t)
    return titles


def _write_wmctrl_snapshots(
    path: Path, before_launch: str, after_launch_wait: str, after_submit: str, after_popups: str, post_stabilize: str, final: str
) -> None:
    _write_text(
        path,
        "[before_launch]\n"
        + str(before_launch or "")
        + ("\n" if before_launch and not before_launch.endswith("\n") else "")
        + "[after_launch_wait]\n"
        + str(after_launch_wait or "")
        + ("\n" if after_launch_wait and not after_launch_wait.endswith("\n") else "")
        + "[after_submit]\n"
        + str(after_submit or "")
        + ("\n" if after_submit and not after_submit.endswith("\n") else "")
        + "[after_popups]\n"
        + str(after_popups or "")
        + ("\n" if after_popups and not after_popups.endswith("\n") else "")
        + "[post_stabilize]\n"
        + str(post_stabilize or "")
        + ("\n" if post_stabilize and not post_stabilize.endswith("\n") else "")
        + "[final]\n"
        + str(final or "")
        + ("\n" if final and not final.endswith("\n") else ""),
    )


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
        launch_root = data.get("launch") if isinstance(data.get("launch"), dict) else {}
        providers = launch_root.get("providers") if isinstance(launch_root.get("providers"), list) else []
        direct = next(
            (p for p in providers if isinstance(p, dict) and str(p.get("provider", "")).strip().lower() == "direct"),
            None,
        )
        if isinstance(direct, dict) and isinstance(direct.get("command"), list) and direct.get("command"):
            cmd = [str(x) for x in direct.get("command", [])]
            if not launch.get("cwd") and direct.get("cwd"):
                launch["cwd"] = str(direct.get("cwd"))
            if not isinstance(launch.get("env"), dict) and isinstance(direct.get("env"), dict):
                launch["env"] = {str(k): str(v) for k, v in direct.get("env", {}).items()}
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
    required_tokens = [
        "Warnhinweis",
        "Risikohinweis",
        "Disclaimer",
        "Agreement",
        "Important",
        "API Connection",
        "Connection",
        "Börsenspiegel",
        "Boersenspiegel",
        "börsenspiegel",
        "boersenspiegel",
        "Programm wird geschlossen",
        "Programm wird geschlossen...",
        "win0",
        "Login Messages",
        "Login Message",
        "Login Messenger",
        "Messenger",
        "Messages",
        "IBKR Login Messenger",
        # Dow Jones / Top-10 news popups
        "Dow Jones Heutige Top 10",
        "Dow Jones",
        "Heutige Top 10",
        "Top 10 Today",
    ]
    seen_tokens = {str(t).strip().lower() for t in popup_whitelist if str(t).strip()}
    for tok in required_tokens:
        if tok.lower() not in seen_tokens:
            popup_whitelist.append(tok)
            seen_tokens.add(tok.lower())
    popup_action_map.setdefault("Warnhinweis", "CLICK_WARNHINWEIS")
    popup_action_map.setdefault("Disclaimer", "CLICK_WARNHINWEIS")
    popup_action_map.setdefault("Börsenspiegel", "ALT+F4")
    popup_action_map.setdefault("Boersenspiegel", "ALT+F4")
    popup_action_map.setdefault("börsenspiegel", "ALT+F4")
    popup_action_map.setdefault("boersenspiegel", "ALT+F4")
    popup_action_map.setdefault("Programm wird geschlossen", "ALT+F4")
    popup_action_map.setdefault("Programm wird geschlossen...", "ALT+F4")
    popup_action_map.setdefault("win0", "ALT+F4")
    popup_action_map.setdefault("Login Messages", "ALT+F4")
    popup_action_map.setdefault("Login Message", "ALT+F4")
    popup_action_map.setdefault("Login Messenger", "ALT+F4")
    popup_action_map.setdefault("Messenger", "ALT+F4")
    popup_action_map.setdefault("Messages", "ALT+F4")
    popup_action_map.setdefault("IBKR Login Messenger", "ALT+F4")
    popup_action_map.setdefault("Risikohinweis", "CLICK_WARNHINWEIS")
    popup_action_map.setdefault("Dow Jones Heutige Top 10", "ALT+F4")
    popup_action_map.setdefault("Dow Jones", "ALT+F4")
    popup_action_map.setdefault("Heutige Top 10", "ALT+F4")
    popup_action_map.setdefault("Top 10 Today", "ALT+F4")
    ibkr = data.get("ibkr") if isinstance(data.get("ibkr"), dict) else {}
    api_host = str(ibkr.get("host") or "127.0.0.1")
    api_port = int(ibkr.get("port") or 7497)

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
        api_host=api_host,
        api_port=api_port,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="TWS X11 autologin chain with deterministic preflight and popup whitelist.")
    ap.add_argument("--preflight-only", action="store_true", default=False)
    ap.add_argument("--drain-only", action="store_true", default=False)
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
    _write_text(ui_events, "")
    _set_ui_events_path(ui_events)
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
    configured_display = str(cfg.launch_env.get("DISPLAY") or "").strip()
    chosen_display = configured_display or ":0"
    root_env["DISPLAY"] = chosen_display
    if "XAUTHORITY" in cfg.launch_env:
        root_env["XAUTHORITY"] = str(cfg.launch_env.get("XAUTHORITY") or "")

    cp_xdpy = _run(["xdpyinfo", "-display", chosen_display], timeout=4.0, env=root_env)
    cp_xset = _run(["xset", "q"], timeout=4.0, env=root_env)
    display_diag = {
        "display": chosen_display,
        "xauthority": str(root_env.get("XAUTHORITY") or ""),
        "xdpyinfo_rc": int(cp_xdpy.returncode),
        "xdpyinfo_stderr_tail": _tail(cp_xdpy.stderr or "", 800),
        "xset_rc": int(cp_xset.returncode),
        "xset_stderr_tail": _tail(cp_xset.stderr or "", 800),
    }

    old_display = os.environ.get("DISPLAY")
    old_xauthority = os.environ.get("XAUTHORITY")
    os.environ["DISPLAY"] = chosen_display
    if str(root_env.get("XAUTHORITY") or ""):
        os.environ["XAUTHORITY"] = str(root_env["XAUTHORITY"])
    elif "XAUTHORITY" in os.environ:
        os.environ.pop("XAUTHORITY", None)
    try:
        preflight = run_full_preflight(evidence_root=repo_root / "octa" / "var" / "evidence")
    finally:
        if old_display is None:
            os.environ.pop("DISPLAY", None)
        else:
            os.environ["DISPLAY"] = old_display
        if old_xauthority is None:
            os.environ.pop("XAUTHORITY", None)
        else:
            os.environ["XAUTHORITY"] = old_xauthority

    if cp_xdpy.returncode != 0 or cp_xset.returncode != 0:
        preflight = {
            "ok": False,
            "code": 11,
            "reason": "X_SERVER_UNREACHABLE",
            "actions": ["Run in an authorized desktop shell where xdpyinfo/xset pass for DISPLAY.", "Set XAUTHORITY in environment before running this script."],
            "evidence_dir": "",
        }
    if not bool(preflight.get("ok")):
        reason = str(preflight.get("reason", "X_SERVER_UNREACHABLE"))
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": int(preflight.get("code", 11)),
            "reason": reason,
            "details": {
                "phase": "preflight",
                "x11_preflight_evidence": preflight.get("evidence_dir", ""),
                **display_diag,
            },
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
            {
                "ts_utc": _utc_iso(),
                "ok": True,
                "code": EXIT_OK,
                "reason": "PREFLIGHT_ONLY_OK",
                "details": {
                    "x11_preflight_evidence": preflight.get("evidence_dir", ""),
                    **display_diag,
                },
            },
        )
        _write_text(evidence_dir / "commands.txt", "")
        _write_text(evidence_dir / "wmctrl_windows.txt", "")
        _write_text(evidence_dir / "report.md", "# TWS X11 Autologin Report\n\n- status: PASS\n- reason: PREFLIGHT_ONLY_OK\n")
        _write_text(ui_events, "")
        _sha_manifest(evidence_dir)
        print(json.dumps({"status": "x11_preflight_ok", "x11_preflight_evidence": preflight.get("evidence_dir", ""), "evidence_dir": str(evidence_dir)}, sort_keys=True))
        return EXIT_OK

    user = str(root_env.get(cfg.user_env_name) or "")
    pw = str(root_env.get(cfg.pass_env_name) or "")
    if (not args.drain_only) and (not user or not pw):
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": EXIT_MISSING_CREDENTIALS,
            "reason": "MISSING_CREDENTIALS",
            "details": {"username_env": cfg.user_env_name, "password_env": cfg.pass_env_name, **display_diag},
        }
        _write_json(evidence_dir / "health.json", health)
        _write_text(evidence_dir / "commands.txt", "")
        _write_text(evidence_dir / "wmctrl_windows.txt", "")
        _write_text(evidence_dir / "report.md", "# TWS X11 Autologin Report\n\n- status: FAIL\n- reason: MISSING_CREDENTIALS\n")
        _sha_manifest(evidence_dir)
        print(json.dumps({"evidence_dir": str(evidence_dir), "code": EXIT_MISSING_CREDENTIALS, "reason": "MISSING_CREDENTIALS"}, sort_keys=True))
        return EXIT_MISSING_CREDENTIALS

    # Snapshot windows before launch on the same DISPLAY.
    prelaunch_windows_txt = ""
    after_submit_windows_txt = ""
    after_popups_windows_txt = ""
    post_stabilize_windows_txt = ""
    final_windows_txt = ""
    tws_pid = 0
    login_search_tokens = [cfg.login_title_contains]
    main_search_tokens = _main_detection_tokens(cfg.main_title_contains)
    login_win: dict[str, str] | None = None
    main_win: dict[str, str] | None = None
    last_windows_txt = ""
    last_titles: list[str] = []
    login_title_seen = ""
    main_title_seen = ""
    click_meta: dict[str, Any] = {}

    rc_pre, out_pre = _wmctrl_windows(root_env, commands_log)
    if rc_pre == 0:
        prelaunch_windows_txt = out_pre
    elif args.drain_only:
        _screenshot(root_env, shots_dir / "fail_root.png", commands_log)
        _screenshot(root_env, shots_dir / "final_root.png", commands_log)
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": 11,
            "reason": "X_SERVER_UNREACHABLE",
            "details": {"phase": "drain_only_wmctrl_before", **display_diag},
        }
        _write_json(evidence_dir / "health.json", health)
        _write_text(evidence_dir / "commands.txt", "\n".join(commands_log) + "\n")
        _write_text(evidence_dir / "wmctrl_windows.txt", "")
        _sha_manifest(evidence_dir)
        print(json.dumps({"evidence_dir": str(evidence_dir), "code": 11, "reason": "X_SERVER_UNREACHABLE"}, sort_keys=True))
        return 11

    if args.drain_only:
        rc_now, out_now = _wmctrl_windows(root_env, commands_log)
        if rc_now != 0:
            _screenshot(root_env, shots_dir / "fail_root.png", commands_log)
            _screenshot(root_env, shots_dir / "final_root.png", commands_log)
            health = {
                "ts_utc": _utc_iso(),
                "ok": False,
                "code": 11,
                "reason": "X_SERVER_UNREACHABLE",
                "details": {"phase": "drain_only_wmctrl_scan", **display_diag},
            }
            _write_json(evidence_dir / "health.json", health)
            _write_text(evidence_dir / "commands.txt", "\n".join(commands_log) + "\n")
            _write_text(evidence_dir / "wmctrl_windows.txt", "")
            _sha_manifest(evidence_dir)
            print(json.dumps({"evidence_dir": str(evidence_dir), "code": 11, "reason": "X_SERVER_UNREACHABLE"}, sort_keys=True))
            return 11
        last_windows_txt = out_now
        wins_now = _parse_windows(out_now)
        last_titles = [str(w.get("title", "")) for w in wins_now if str(w.get("title", ""))]
        login_win = _find_window_any(wins_now, login_search_tokens)
        main_candidates = _find_main_windows(wins_now, cfg.main_title_contains)
        main_win = main_candidates[0] if main_candidates else None
        if main_win is not None:
            main_title_seen = str(main_win.get("title", ""))
            try:
                tws_pid = int(str(main_win.get("pid", "0")) or "0")
            except Exception:
                tws_pid = 0
        if login_win is not None:
            login_title_seen = str(login_win.get("title", ""))
    else:
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
                "details": {"error": f"{type(exc).__name__}:{exc}", "command": cfg.launch_cmd, **display_diag},
            }
            _write_json(evidence_dir / "health.json", health)
            _write_text(evidence_dir / "commands.txt", "\n".join(commands_log) + "\n")
            _write_text(evidence_dir / "wmctrl_windows.txt", "")
            _write_text(evidence_dir / "report.md", "# TWS X11 Autologin Report\n\n- status: FAIL\n- reason: LAUNCH_FAILED\n")
            _sha_manifest(evidence_dir)
            print(json.dumps({"evidence_dir": str(evidence_dir), "code": EXIT_LAUNCH_FAILED, "reason": "LAUNCH_FAILED"}, sort_keys=True))
            return EXIT_LAUNCH_FAILED
        tws_pid = int(proc.pid)

        # Wait for main/login window
        deadline = time.monotonic() + max(10, int(args.timeout_sec))
        while time.monotonic() < deadline:
            rc, out = _wmctrl_windows(root_env, commands_log)
            if rc == 0:
                last_windows_txt = out
                wins = _parse_windows(out)
                last_titles = [str(w.get("title", "")) for w in wins if str(w.get("title", ""))]
                login_win = _find_window_any(wins, login_search_tokens)
                main_candidates = _find_main_windows(wins, cfg.main_title_contains)
                main_win = main_candidates[0] if main_candidates else None
                login_title_seen = str(login_win.get("title", "")) if login_win else login_title_seen
                main_title_seen = str(main_win.get("title", "")) if main_win else main_title_seen
                if login_win or main_win:
                    break
            time.sleep(0.5)

    _write_wmctrl_snapshots(
        evidence_dir / "wmctrl_windows.txt",
        prelaunch_windows_txt,
        last_windows_txt,
        after_submit_windows_txt,
        after_popups_windows_txt,
        post_stabilize_windows_txt,
        final_windows_txt,
    )
    _screenshot(root_env, shots_dir / "before_login.png", commands_log)

    if (not args.drain_only) and (not login_win and not main_win):
        _screenshot(root_env, shots_dir / "fail_root.png", commands_log)
        _screenshot(root_env, shots_dir / "final_root.png", commands_log)
        rc_fin, out_fin = _wmctrl_windows(root_env, commands_log)
        if rc_fin == 0:
            final_windows_txt = out_fin
        _write_wmctrl_snapshots(
            evidence_dir / "wmctrl_windows.txt",
            prelaunch_windows_txt,
            last_windows_txt,
            after_submit_windows_txt,
            after_popups_windows_txt,
            post_stabilize_windows_txt,
            final_windows_txt,
        )
        details: dict[str, Any] = {
            "tws_pid": tws_pid,
            "login_title_contains": cfg.login_title_contains,
            "main_title_contains": cfg.main_title_contains,
            "searched_login_substrings": login_search_tokens,
            "searched_main_substrings": main_search_tokens,
            "window_titles_seen": last_titles,
            "window_enum_method": "wmctrl",
            "login_window_title_seen": login_title_seen,
            "main_window_title_seen": main_title_seen,
            **display_diag,
        }
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": EXIT_WINDOW_NOT_FOUND,
            "reason": "WINDOW_NOT_FOUND",
            "details": details,
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
    if (not args.drain_only) and login_win:
        _focus_window(login_win["wid"], root_env, commands_log)
        login_xwid = _wid_for_xdotool(login_win["wid"])
        commands_log.append(f"xdotool windowfocus {login_xwid}")
        _run_gui_cmd(
            ["xdotool", "windowfocus", str(login_xwid)],
            timeout=4.0,
            env=root_env,
            state="LOGIN",
            step="xdotool_windowfocus",
            wid_wmctrl=str(login_win["wid"]),
            wid_xdotool=str(login_xwid),
        )
        time.sleep(0.2)
        _, click_meta = _click_focus_in_login(login_win["wid"], root_env, commands_log)
        time.sleep(0.2)
        for _ in range(max(0, int(cfg.tab_to_username))):
            _key("Tab", root_env, commands_log)
        _type_text(user, root_env, commands_log)
        for _ in range(max(0, int(cfg.tab_to_password))):
            _key("Tab", root_env, commands_log)
        _type_text(pw, root_env, commands_log)
        _screenshot(root_env, shots_dir / "after_typing.png", commands_log)
        _key(cfg.submit_key, root_env, commands_log)
        time.sleep(0.2)
        _screenshot(root_env, shots_dir / "after_submit.png", commands_log)
        rc_post, out_post = _wmctrl_windows(root_env, commands_log)
        if rc_post == 0:
            after_submit_windows_txt = out_post

    # Popup state machine
    state_trace: list[dict[str, Any]] = []
    popups_closed: list[str] = []
    popups_confirmed: list[str] = []
    popup_deadline = time.monotonic() + max(1, int(cfg.popup_timeout_sec))
    max_iters = 60
    popup_iters = 0
    state = STATE_CLASSIFY
    unknown_popup: WindowInfo | None = None
    unknown_titles: list[str] = []
    titles_seen_latest: list[str] = []
    popup_candidates: list[WindowInfo] = []
    main_candidates_w: list[WindowInfo] = []
    login_exact_w: WindowInfo | None = None
    consecutive_clear = 0
    current_target: WindowInfo | None = None
    current_action = ""
    current_ok = False
    current_stderr = ""

    while time.monotonic() < popup_deadline and popup_iters < max_iters:
        popup_iters += 1
        if state == STATE_RECHECK:
            _log_state_transition(STATE_RECHECK, STATE_CLASSIFY, reason="loop")
            state = STATE_CLASSIFY
        if state == STATE_CLASSIFY:
            unknown_popup = None
            unknown_titles = []
            rc, out, win_objs = _list_windows_wmctrl(root_env, commands_log)
            if rc != 0:
                state_trace.append({"ts_utc": _utc_iso(), "state": state, "action": "wmctrl_list", "result": f"rc={rc}"})
                _log_state_transition(STATE_CLASSIFY, STATE_STABILIZE, reason=f"wmctrl_rc_{rc}")
                state = STATE_STABILIZE
                continue
            after_popups_windows_txt = out
            xpopup = _xdotool_find_windows_by_tokens(root_env, commands_log, cfg.popup_whitelist)
            seen_dec = {int(w.wid_dec) for w in win_objs if int(w.wid_dec) >= 0}
            for xw in xpopup:
                try:
                    xdec = int(str(xw.get("wid", "")).strip())
                except Exception:
                    continue
                if xdec in seen_dec:
                    continue
                xtitle = str(xw.get("title", ""))
                mtok = str(xw.get("matched_token", ""))
                win_objs.append(
                    WindowInfo(
                        wid_hex=hex(xdec),
                        wid_dec=xdec,
                        desktop="",
                        title=xtitle,
                        pid="0",
                        source="xdotool",
                        matched_tokens=(mtok,) if mtok else (),
                    )
                )
                seen_dec.add(xdec)
            titles_seen = [w.title for w in win_objs if w.title]
            titles_seen_latest = list(titles_seen)
            main_candidates_w = [w for w in win_objs if _classify_window(w, cfg, main_search_tokens) == WIN_MAIN]
            if main_candidates_w:
                main_win = {"wid": main_candidates_w[0].wid_hex, "title": main_candidates_w[0].title}
                main_title_seen = main_candidates_w[0].title or main_title_seen
            login_exact_w = next((w for w in win_objs if _classify_window(w, cfg, main_search_tokens) == WIN_LOGIN), None)
            login_win = {"wid": login_exact_w.wid_hex, "title": login_exact_w.title} if login_exact_w is not None else None
            if login_exact_w is not None:
                login_title_seen = login_exact_w.title or login_title_seen

            popup_candidates = []
            unknown_wins: list[WindowInfo] = []
            for w in win_objs:
                if not w.matched_tokens:
                    mt = _first_matching_token(w.title, cfg.popup_whitelist)
                    if mt:
                        w = WindowInfo(
                            wid_hex=w.wid_hex,
                            wid_dec=w.wid_dec,
                            desktop=w.desktop,
                            title=w.title,
                            pid=w.pid,
                            source=w.source,
                            matched_tokens=(mt,),
                        )
                if any(m.wid_hex == w.wid_hex for m in main_candidates_w):
                    continue
                if login_exact_w is not None and w.wid_hex == login_exact_w.wid_hex:
                    continue
                matches_token = _matches_any_token(w.title, BLOCKING_TOKENS) or bool(w.matched_tokens)
                pid_match = _pid_matches_tws_or_zero(w.pid, tws_pid)
                cat = _classify_window(w, cfg, main_search_tokens)
                if matches_token and pid_match and cat in {WIN_POPUP_WARN, WIN_POPUP_DISCLAIMER, WIN_POPUP_LOGIN_MESSAGES, WIN_TRANSIENT_CLOSING}:
                    popup_candidates.append(w)
                    continue
                if matches_token and pid_match:
                    popup_candidates.append(w)
                    continue
                if str(w.pid).strip() == str(tws_pid) and w.title:
                    unknown_wins.append(w)
            if unknown_wins:
                unknown_popup = unknown_wins[0]
                unknown_titles = [w.title for w in unknown_wins if w.title]
            state_trace.append(
                {
                    "ts_utc": _utc_iso(),
                    "state": STATE_CLASSIFY,
                    "action": "enumerate",
                    "target_title": unknown_popup.title if unknown_popup else "",
                    "target_wid_hex": unknown_popup.wid_hex if unknown_popup else "",
                    "result": f"titles={len(titles_seen)} popups={len(popup_candidates)} unknown={len(unknown_wins)}",
                }
            )
            _log_state_transition(STATE_CLASSIFY, STATE_ROUTE, reason="classified", target=unknown_popup.title if unknown_popup else "", wid=unknown_popup.wid_hex if unknown_popup else "")
            state = STATE_ROUTE
            continue

        if state == STATE_ROUTE:
            if unknown_popup is not None and not popup_candidates:
                _log_state_transition(STATE_ROUTE, STATE_FAIL, reason="unknown_popup", target=unknown_popup.title, wid=unknown_popup.wid_hex)
                state = STATE_FAIL
                continue
            if popup_candidates:
                def _prio(w: WindowInfo) -> int:
                    t = w.title.lower()
                    # prio 0: Börsenspiegel market news
                    if "börsenspiegel" in t or "boersenspiegel" in t:
                        return 0
                    # prio 1: Login Messages
                    if (
                        "login messages" in t
                        or "login message" in t
                        or "login messenger" in t
                        or "ibkr login messenger" in t
                        or t == "messages"
                        or " messenger" in t
                    ):
                        return 1
                    # prio 2: transient closing / artefact windows
                    if "programm wird geschlossen" in t or t == "win0":
                        return 2
                    # prio 2: Dow Jones / Top 10 news (same urgency as transient)
                    if "dow jones" in t or "heutige top 10" in t or "top 10 today" in t:
                        return 2
                    # prio 3: Disclaimer / Warnhinweis / Risikohinweis
                    if "warnhinweis" in t or "risikohinweis" in t or "disclaimer" in t:
                        return 3
                    return 9

                popup_candidates = sorted(popup_candidates, key=lambda w: (_prio(w), w.title.lower()))
                current_target = popup_candidates[0]
                ctitle = current_target.title.lower()
                if "warnhinweis" in ctitle or "risikohinweis" in ctitle or "disclaimer" in ctitle:
                    current_action = "CLICK_WARNHINWEIS"
                else:
                    current_action = "ALT+F4"
                state_trace.append(
                    {
                        "ts_utc": _utc_iso(),
                        "state": STATE_ROUTE,
                        "action": current_action,
                        "target_title": current_target.title,
                        "target_wid_hex": current_target.wid_hex,
                        "result": "selected",
                    }
                )
                _log_state_transition(STATE_ROUTE, STATE_ACT, reason=current_action, target=current_target.title, wid=current_target.wid_hex)
                state = STATE_ACT
                continue
            consecutive_clear += 1
            if consecutive_clear >= 3:
                _log_state_transition(STATE_ROUTE, STATE_DONE, reason="clear_consecutive_3")
                state = STATE_DONE
            else:
                _log_state_transition(STATE_ROUTE, STATE_STABILIZE, reason=f"clear_consecutive_{consecutive_clear}")
                state = STATE_STABILIZE
            continue

        if state == STATE_ACT:
            current_ok = False
            current_stderr = ""
            if current_target is None:
                _log_state_transition(STATE_ACT, STATE_FAIL, reason="missing_target")
                state = STATE_FAIL
                continue
            wid_raw = current_target.wid_hex
            matched_token = current_target.matched_tokens[0] if current_target.matched_tokens else _first_matching_token(
                current_target.title, cfg.popup_whitelist
            )
            confirm_tokens = [matched_token] if matched_token else [current_target.title]
            resolved_xwid = _resolve_xdotool_wid(root_env, commands_log, current_target.title, tuple(confirm_tokens))
            if current_action == "CLICK_WARNHINWEIS":
                cp_act = _wmctrl_activate(wid_raw, root_env, commands_log)
                _step_event(
                    ui_events,
                    state=STATE_ACT,
                    action=current_action,
                    step="wmctrl_activate",
                    title=current_target.title,
                    token=matched_token,
                    wid_wmctrl=wid_raw,
                    wid_xdotool=resolved_xwid,
                    cmd=f"wmctrl -ia {wid_raw}",
                    rc=cp_act.returncode,
                    stderr_tail=_tail(cp_act.stderr or "", 400),
                )
                geom = _xwininfo_geometry(wid_raw, root_env, commands_log)
                gx = int(geom.get("x", 0))
                gy = int(geom.get("y", 0))
                gw = max(1, int(geom.get("width", 1)))
                gh = max(1, int(geom.get("height", 1)))
                p1x = gx + max(1, int(gw * 0.85))
                p1y = gy + max(1, int(gh * 0.88))
                p2x = gx + max(1, int(gw * 0.50))
                p2y = gy + max(1, int(gh * 0.88))

                ok_click1 = _abs_click(p1x, p1y, root_env, commands_log)
                _step_event(
                    ui_events,
                    state=STATE_ACT,
                    action=current_action,
                    step="click_85_88",
                    title=current_target.title,
                    token=matched_token,
                    wid_wmctrl=wid_raw,
                    wid_xdotool=resolved_xwid,
                    cmd=f"xdotool mousemove {p1x} {p1y}; xdotool click 1",
                    rc=0 if ok_click1 else 1,
                    click_x=p1x,
                    click_y=p1y,
                )
                cp_ret1 = _run_gui_cmd(
                    ["xdotool", "key", "Return"],
                    timeout=4.0,
                    env=root_env,
                    state=STATE_ACT,
                    step="key_return_1",
                    title=current_target.title,
                    token=matched_token,
                    wid_wmctrl=wid_raw,
                    wid_xdotool=resolved_xwid,
                )
                _step_event(
                    ui_events,
                    state=STATE_ACT,
                    action=current_action,
                    step="key_return_1",
                    title=current_target.title,
                    token=matched_token,
                    wid_wmctrl=wid_raw,
                    wid_xdotool=resolved_xwid,
                    cmd="xdotool key Return",
                    rc=cp_ret1.returncode,
                    stderr_tail=_tail(cp_ret1.stderr or "", 400),
                )
                closed_now, streak = _confirm_absent(root_env, commands_log, confirm_tokens, polls=3, tws_pid=tws_pid)
                _step_event(
                    ui_events,
                    state=STATE_ACT,
                    action=current_action,
                    step="confirm_after_1",
                    title=current_target.title,
                    token=matched_token,
                    wid_wmctrl=wid_raw,
                    wid_xdotool=resolved_xwid,
                    cmd="wmctrl -lp (confirm absent)",
                    rc=0 if closed_now else 1,
                    confirm_result="closed" if closed_now else "still_present",
                    confirm_streak=streak,
                )
                if not closed_now:
                    ok_click2 = _abs_click(p2x, p2y, root_env, commands_log)
                    _step_event(
                        ui_events,
                        state=STATE_ACT,
                        action=current_action,
                        step="click_50_88",
                        title=current_target.title,
                        token=matched_token,
                        wid_wmctrl=wid_raw,
                        wid_xdotool=resolved_xwid,
                        cmd=f"xdotool mousemove {p2x} {p2y}; xdotool click 1",
                        rc=0 if ok_click2 else 1,
                        click_x=p2x,
                        click_y=p2y,
                    )
                    cp_ret2 = _run_gui_cmd(
                        ["xdotool", "key", "Return"],
                        timeout=4.0,
                        env=root_env,
                        state=STATE_ACT,
                        step="key_return_2",
                        title=current_target.title,
                        token=matched_token,
                        wid_wmctrl=wid_raw,
                        wid_xdotool=resolved_xwid,
                    )
                    _step_event(
                        ui_events,
                        state=STATE_ACT,
                        action=current_action,
                        step="key_return_2",
                        title=current_target.title,
                        token=matched_token,
                        wid_wmctrl=wid_raw,
                        wid_xdotool=resolved_xwid,
                        cmd="xdotool key Return",
                        rc=cp_ret2.returncode,
                        stderr_tail=_tail(cp_ret2.stderr or "", 400),
                    )
                    closed_now, streak = _confirm_absent(root_env, commands_log, confirm_tokens, polls=3, tws_pid=tws_pid)
                    _step_event(
                        ui_events,
                        state=STATE_ACT,
                        action=current_action,
                        step="confirm_after_2",
                        title=current_target.title,
                        token=matched_token,
                        wid_wmctrl=wid_raw,
                        wid_xdotool=resolved_xwid,
                        cmd="wmctrl -lp (confirm absent)",
                        rc=0 if closed_now else 1,
                        confirm_result="closed" if closed_now else "still_present",
                        confirm_streak=streak,
                    )
                if not closed_now:
                    cp_space = _run_gui_cmd(
                        ["xdotool", "key", "space"],
                        timeout=4.0,
                        env=root_env,
                        state=STATE_ACT,
                        step="key_space",
                        title=current_target.title,
                        token=matched_token,
                        wid_wmctrl=wid_raw,
                        wid_xdotool=resolved_xwid,
                    )
                    _step_event(
                        ui_events,
                        state=STATE_ACT,
                        action=current_action,
                        step="key_space",
                        title=current_target.title,
                        token=matched_token,
                        wid_wmctrl=wid_raw,
                        wid_xdotool=resolved_xwid,
                        cmd="xdotool key space",
                        rc=cp_space.returncode,
                        stderr_tail=_tail(cp_space.stderr or "", 400),
                    )
                    cp_ret3 = _run_gui_cmd(
                        ["xdotool", "key", "Return"],
                        timeout=4.0,
                        env=root_env,
                        state=STATE_ACT,
                        step="key_return_3",
                        title=current_target.title,
                        token=matched_token,
                        wid_wmctrl=wid_raw,
                        wid_xdotool=resolved_xwid,
                    )
                    _step_event(
                        ui_events,
                        state=STATE_ACT,
                        action=current_action,
                        step="key_return_3",
                        title=current_target.title,
                        token=matched_token,
                        wid_wmctrl=wid_raw,
                        wid_xdotool=resolved_xwid,
                        cmd="xdotool key Return",
                        rc=cp_ret3.returncode,
                        stderr_tail=_tail(cp_ret3.stderr or "", 400),
                    )
                    closed_now, streak = _confirm_absent(root_env, commands_log, confirm_tokens, polls=3, tws_pid=tws_pid)
                    _step_event(
                        ui_events,
                        state=STATE_ACT,
                        action=current_action,
                        step="confirm_after_3",
                        title=current_target.title,
                        token=matched_token,
                        wid_wmctrl=wid_raw,
                        wid_xdotool=resolved_xwid,
                        cmd="wmctrl -lp (confirm absent)",
                        rc=0 if closed_now else 1,
                        confirm_result="closed" if closed_now else "still_present",
                        confirm_streak=streak,
                    )
                current_ok = bool(closed_now)
                _screenshot(root_env, shots_dir / "after_warnhinweis.png", commands_log)
            else:
                cp_act = _wmctrl_activate(wid_raw, root_env, commands_log)
                cp_close = _wmctrl_close(wid_raw, root_env, commands_log)
                current_ok = cp_close.returncode == 0
                current_stderr = _tail(cp_close.stderr or "", 400)
                _step_event(
                    ui_events,
                    state=STATE_ACT,
                    action=current_action,
                    step="wmctrl_activate",
                    title=current_target.title,
                    token=matched_token,
                    wid_wmctrl=wid_raw,
                    wid_xdotool=resolved_xwid,
                    cmd=f"wmctrl -ia {wid_raw}",
                    rc=cp_act.returncode,
                    stderr_tail=_tail(cp_act.stderr or "", 400),
                )
                _step_event(
                    ui_events,
                    state=STATE_ACT,
                    action=current_action,
                    step="wmctrl_ic",
                    title=current_target.title,
                    token=matched_token,
                    wid_wmctrl=wid_raw,
                    wid_xdotool=resolved_xwid,
                    cmd=f"wmctrl -ic {wid_raw}",
                    rc=cp_close.returncode,
                    stderr_tail=current_stderr,
                )
                closed_after_wmctrl, streak_wmctrl = _confirm_absent(root_env, commands_log, confirm_tokens, polls=3, tws_pid=tws_pid)
                _step_event(
                    ui_events,
                    state=STATE_ACT,
                    action=current_action,
                    step="confirm_after_wmctrl_ic",
                    title=current_target.title,
                    token=matched_token,
                    wid_wmctrl=wid_raw,
                    wid_xdotool=resolved_xwid,
                    cmd="wmctrl -lp (confirm absent)",
                    rc=0 if closed_after_wmctrl else 1,
                    confirm_result="closed" if closed_after_wmctrl else "still_present",
                    confirm_streak=streak_wmctrl,
                )
                if not closed_after_wmctrl:
                    token_probes = [t for t in confirm_tokens if str(t).strip()]
                    if not token_probes:
                        token_probes = [current_target.title]
                    xwins = _xdotool_find_windows_by_tokens(root_env, commands_log, token_probes)
                    allowed_wids: set[str] = set()
                    rc_allow, out_allow = _wmctrl_windows(root_env, commands_log)
                    if rc_allow == 0:
                        for aw in _parse_windows(out_allow):
                            atitle = str(aw.get("title", ""))
                            apid = str(aw.get("pid", ""))
                            if _matches_any_token(atitle, token_probes) and _pid_matches_tws_or_zero(apid, tws_pid):
                                allowed_wids.add(_wid_hex_norm(str(aw.get("wid", ""))))
                    for xw in xwins:
                        xwid = str(xw.get("wid", "")).strip()
                        if not xwid:
                            continue
                        if allowed_wids and _wid_hex_norm(xwid) not in allowed_wids:
                            continue
                        commands_log.append(f"xdotool windowactivate --sync {xwid}")
                        cp1 = _run_gui_cmd(
                            ["xdotool", "windowactivate", "--sync", xwid],
                            timeout=4.0,
                            env=root_env,
                            state=STATE_ACT,
                            step="xdotool_windowactivate_sync",
                            title=current_target.title,
                            token=matched_token,
                            wid_wmctrl=wid_raw,
                            wid_xdotool=xwid,
                        )
                        _step_event(
                            ui_events,
                            state=STATE_ACT,
                            action=current_action,
                            step="xdotool_escape",
                            title=current_target.title,
                            token=matched_token,
                            wid_wmctrl=wid_raw,
                            wid_xdotool=xwid,
                            cmd=f"xdotool key --window {xwid} Escape",
                            rc=_run_gui_cmd(
                                ["xdotool", "key", "--window", xwid, "Escape"],
                                timeout=4.0,
                                env=root_env,
                                state=STATE_ACT,
                                step="xdotool_escape",
                                title=current_target.title,
                                token=matched_token,
                                wid_wmctrl=wid_raw,
                                wid_xdotool=xwid,
                            ).returncode,
                        )
                        _step_event(
                            ui_events,
                            state=STATE_ACT,
                            action=current_action,
                            step="xdotool_windowactivate_sync",
                            title=current_target.title,
                            token=matched_token,
                            wid_wmctrl=wid_raw,
                            wid_xdotool=xwid,
                            cmd=f"xdotool windowactivate --sync {xwid}",
                            rc=cp1.returncode,
                            stderr_tail=_tail(cp1.stderr or "", 400),
                        )
                        commands_log.append(f"xdotool key --window {xwid} alt+F4")
                        cp2 = _run_gui_cmd(
                            ["xdotool", "key", "--window", xwid, "alt+F4"],
                            timeout=4.0,
                            env=root_env,
                            state=STATE_ACT,
                            step="xdotool_altf4",
                            title=current_target.title,
                            token=matched_token,
                            wid_wmctrl=wid_raw,
                            wid_xdotool=xwid,
                        )
                        _step_event(
                            ui_events,
                            state=STATE_ACT,
                            action=current_action,
                            step="xdotool_altf4",
                            title=current_target.title,
                            token=matched_token,
                            wid_wmctrl=wid_raw,
                            wid_xdotool=xwid,
                            cmd=f"xdotool key --window {xwid} alt+F4",
                            rc=cp2.returncode,
                            stderr_tail=_tail(cp2.stderr or "", 400),
                        )
                        commands_log.append(f"xdotool windowclose {xwid}")
                        cp3 = _run_gui_cmd(
                            ["xdotool", "windowclose", xwid],
                            timeout=4.0,
                            env=root_env,
                            state=STATE_ACT,
                            step="xdotool_windowclose",
                            title=current_target.title,
                            token=matched_token,
                            wid_wmctrl=wid_raw,
                            wid_xdotool=xwid,
                        )
                        _step_event(
                            ui_events,
                            state=STATE_ACT,
                            action=current_action,
                            step="xdotool_windowclose",
                            title=current_target.title,
                            token=matched_token,
                            wid_wmctrl=wid_raw,
                            wid_xdotool=xwid,
                            cmd=f"xdotool windowclose {xwid}",
                            rc=cp3.returncode,
                            stderr_tail=_tail(cp3.stderr or "", 400),
                        )
                    _wmctrl_close(wid_raw, root_env, commands_log)
                current_ok, final_streak = _confirm_absent(root_env, commands_log, confirm_tokens, polls=3, tws_pid=tws_pid)
                _step_event(
                    ui_events,
                    state=STATE_ACT,
                    action=current_action,
                    step="confirm_after_xdotool_fallback",
                    title=current_target.title,
                    token=matched_token,
                    wid_wmctrl=wid_raw,
                    wid_xdotool=resolved_xwid,
                    cmd="wmctrl -lp (confirm absent)",
                    rc=0 if current_ok else 1,
                    confirm_result="closed" if current_ok else "still_present",
                    confirm_streak=final_streak,
                )
            _log_state_transition(STATE_ACT, STATE_CONFIRM, reason="action_complete", target=current_target.title, wid=current_target.wid_hex)
            state = STATE_CONFIRM
            continue

        if state == STATE_CONFIRM:
            if current_target is None:
                _log_state_transition(STATE_CONFIRM, STATE_FAIL, reason="missing_target")
                state = STATE_FAIL
                continue
            confirm_token = current_target.matched_tokens[0] if current_target.matched_tokens else _first_matching_token(
                current_target.title, cfg.popup_whitelist
            )
            confirm_tokens = [confirm_token] if confirm_token else [current_target.title]
            closed, confirm_streak = _confirm_absent(root_env, commands_log, confirm_tokens, polls=3, tws_pid=tws_pid)
            _step_event(
                ui_events,
                state=STATE_CONFIRM,
                action=current_action,
                step="confirm_absent",
                title=current_target.title,
                token=confirm_token,
                wid_wmctrl=current_target.wid_hex,
                wid_xdotool=str(current_target.wid_dec if current_target.wid_dec >= 0 else ""),
                cmd="wmctrl -lp (confirm absent)",
                rc=0 if closed else 1,
                confirm_result="closed" if closed else "still_present",
                confirm_streak=confirm_streak,
            )
            state_trace.append(
                {
                    "ts_utc": _utc_iso(),
                    "state": STATE_CONFIRM,
                    "action": current_action,
                    "target_title": current_target.title,
                    "target_wid_hex": current_target.wid_hex,
                    "result": "closed" if closed else "still_present",
                }
            )
            if closed:
                consecutive_clear = 0
                if current_action == "CLICK_WARNHINWEIS":
                    popups_confirmed.append(current_target.title)
                else:
                    popups_closed.append(current_target.title)
                _log_state_transition(STATE_CONFIRM, STATE_RECHECK, reason="confirmed_closed", target=current_target.title, wid=current_target.wid_hex)
                state = STATE_RECHECK
            else:
                fail_reason = "POPUP_TIMEOUT_WARN" if current_action == "CLICK_WARNHINWEIS" else "POPUP_TIMEOUT_LOGIN_MESSAGES"
                health = {
                    "ts_utc": _utc_iso(),
                    "ok": False,
                    "code": EXIT_LOGIN_FAILED,
                    "reason": fail_reason,
                    "details": {
                        "window_titles_seen": [w.title for w in popup_candidates],
                        "tws_pid": tws_pid,
                        "popup_whitelist_used": cfg.popup_whitelist,
                        "main_title_contains": cfg.main_title_contains,
                        "main_detection_tokens_used": main_search_tokens,
                        "state_trace": state_trace[-200:],
                        "popups_closed": popups_closed,
                        "popups_confirmed": popups_confirmed,
                        "popups_closed_count": len(popups_closed),
                        "popups_confirmed_count": len(popups_confirmed),
                        "popups_blocking_final": [
                            {"wid_hex": w.wid_hex, "desktop": w.desktop, "pid": w.pid, "title": w.title, "source": w.source}
                            for w in popup_candidates
                            if w.title
                        ],
                        **display_diag,
                    },
                }
                _screenshot(root_env, shots_dir / "fail_root.png", commands_log)
                _screenshot(root_env, shots_dir / "final_root.png", commands_log)
                _write_json(evidence_dir / "health.json", health)
                _write_text(evidence_dir / "commands.txt", "\n".join(commands_log) + "\n")
                _sha_manifest(evidence_dir)
                print(json.dumps({"evidence_dir": str(evidence_dir), "code": EXIT_LOGIN_FAILED, "reason": fail_reason}, sort_keys=True))
                return EXIT_LOGIN_FAILED
            continue

        if state == STATE_STABILIZE:
            time.sleep(0.35)
            state_trace.append({"ts_utc": _utc_iso(), "state": STATE_STABILIZE, "action": "sleep", "result": "ok"})
            _log_state_transition(STATE_STABILIZE, STATE_RECHECK, reason="stabilized")
            state = STATE_RECHECK
            continue

        if state == STATE_DONE:
            break

        if state == STATE_FAIL:
            break

    if unknown_popup is not None or state == STATE_FAIL:
        _screenshot(root_env, shots_dir / "unknown_popup.png", commands_log)
        _screenshot(root_env, shots_dir / "final_root.png", commands_log)
        rc_fin, out_fin = _wmctrl_windows(root_env, commands_log)
        if rc_fin == 0:
            final_windows_txt = out_fin
        _write_wmctrl_snapshots(
            evidence_dir / "wmctrl_windows.txt",
            prelaunch_windows_txt,
            last_windows_txt,
            after_submit_windows_txt,
            after_popups_windows_txt,
            post_stabilize_windows_txt,
            final_windows_txt,
        )
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": EXIT_UNKNOWN_POPUP,
            "reason": "UNKNOWN_POPUP",
            "details": {
                "title": unknown_popup.title if unknown_popup is not None else "",
                "wid": unknown_popup.wid_hex if unknown_popup is not None else "",
                "unknown_title": unknown_popup.title if unknown_popup is not None else "",
                "unknown_wid": unknown_popup.wid_hex if unknown_popup is not None else "",
                "window_titles_seen": titles_seen_latest,
                "popup_whitelist_used": cfg.popup_whitelist,
                "tws_pid": tws_pid,
                "main_title_contains": cfg.main_title_contains,
                "main_detection_tokens_used": main_search_tokens,
                "unknown_titles": unknown_titles,
                "state_trace": state_trace[-200:],
                "popups_closed": popups_closed,
                "popups_confirmed": popups_confirmed,
                "popups_closed_count": len(popups_closed),
                "popups_confirmed_count": len(popups_confirmed),
                "popups_blocking_final": [
                    {"wid_hex": w.wid_hex, "desktop": w.desktop, "pid": w.pid, "title": w.title, "source": w.source}
                    for w in popup_candidates
                    if w.title
                ],
                **display_diag,
            },
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

    # Stabilize UI after popup drain and snapshot once more.
    time.sleep(2.0)
    rc_stab, out_stab = _wmctrl_windows(root_env, commands_log)
    if rc_stab == 0:
        post_stabilize_windows_txt = out_stab

    # Final success condition: drain-only requires empty blocking set; full mode keeps login/main checks.
    api_check_result = "closed"
    api_check_last_error = ""
    api_open = False
    blocking_tokens = list(BLOCKING_TOKENS)
    blocking_titles_present: list[dict[str, str]] = []
    confirm_deadline = time.monotonic() + max(1, int(args.timeout_sec))
    wins: list[dict[str, str]] = []
    stable_ok = 0  # consecutive clean-poll counter; must reach _STABLE_OK_NEEDED before break
    while time.monotonic() < confirm_deadline:
        rc, out = _wmctrl_windows(root_env, commands_log)
        if rc != 0:
            _screenshot(root_env, shots_dir / "fail_root.png", commands_log)
            _screenshot(root_env, shots_dir / "final_root.png", commands_log)
            health = {
                "ts_utc": _utc_iso(),
                "ok": False,
                "code": 11,
                "reason": "X_SERVER_UNREACHABLE",
                "details": {"phase": "final_sweep", "tws_pid": tws_pid, **display_diag},
            }
            _write_json(evidence_dir / "health.json", health)
            _write_text(evidence_dir / "commands.txt", "\n".join(commands_log) + "\n")
            _sha_manifest(evidence_dir)
            print(json.dumps({"evidence_dir": str(evidence_dir), "code": 11, "reason": "X_SERVER_UNREACHABLE"}, sort_keys=True))
            return 11
        final_windows_txt = out
        wins = _parse_windows(out)
        main_candidates = _find_main_windows(wins, cfg.main_title_contains)
        main_win = main_candidates[0] if main_candidates else None
        login_win = _find_window_any(wins, login_search_tokens)
        if login_win is not None:
            login_title_seen = str(login_win.get("title", "")) or login_title_seen
        if main_win is not None:
            main_title_seen = str(main_win.get("title", "")) or main_title_seen
        blocking_titles_present = _collect_blocking_popups_for_pid(root_env, commands_log, blocking_tokens, tws_pid)
        api_open, api_check_last_error = _api_connectable(cfg.api_host, cfg.api_port)
        api_check_result = "open" if api_open else "closed"
        # clean_now: all success conditions satisfied on this poll.
        # Both drain-only and full mode require _STABLE_OK_NEEDED consecutive
        # clean polls to guard against popups appearing between the popup-drain
        # phase and the final sweep (the race that caused false-OK returns).
        _clean_now = (args.drain_only and not blocking_titles_present) or (
            (not args.drain_only)
            and bool(main_win or api_open)
            and not login_win
            and not blocking_titles_present
        )
        if _clean_now:
            stable_ok += 1
            if stable_ok >= _STABLE_OK_NEEDED:
                break
        else:
            stable_ok = 0
        time.sleep(0.5)
    _write_wmctrl_snapshots(
        evidence_dir / "wmctrl_windows.txt",
        prelaunch_windows_txt,
        last_windows_txt,
        after_submit_windows_txt,
        after_popups_windows_txt,
        post_stabilize_windows_txt,
        final_windows_txt,
    )
    blocking_popup_entries = _final_blocking_sweep(root_env, commands_log, blocking_tokens, tws_pid)
    success_ready = (not blocking_popup_entries) if args.drain_only else ((bool(main_win) or bool(api_open)) and (not login_win) and (not blocking_popup_entries))
    if not success_ready:
        _screenshot(root_env, shots_dir / "fail_root.png", commands_log)
        _screenshot(root_env, shots_dir / "final_root.png", commands_log)
        if blocking_popup_entries:
            fail_reason = "POPUP_STILL_PRESENT"
        else:
            fail_reason = "LOGIN_STILL_PRESENT" if api_open and login_win else "LOGIN_NOT_CONFIRMED"
        health = {
            "ts_utc": _utc_iso(),
            "ok": False,
            "code": EXIT_LOGIN_FAILED,
            "reason": fail_reason,
            "details": {
                "login_title_contains": cfg.login_title_contains,
                "main_title_contains": cfg.main_title_contains,
                "searched_login_substrings": login_search_tokens,
                "searched_main_substrings": main_search_tokens,
                "window_titles_seen": [str(w.get("title", "")) for w in wins if str(w.get("title", ""))],
                "login_window_title_seen": login_title_seen,
                "main_window_title_seen": main_title_seen,
                "api_check_host": cfg.api_host,
                "api_check_port": int(cfg.api_port),
                "api_check_result": api_check_result,
                "api_check_last_error": api_check_last_error,
                "tws_pid": tws_pid,
                "blocking_popup_titles_present": [str(x.get("title", "")) for x in blocking_popup_entries],
                "popups_blocking_final": blocking_popup_entries,
                "state_trace": state_trace[-200:],
                "popups_closed": popups_closed,
                "popups_confirmed": popups_confirmed,
                "popups_closed_count": len(popups_closed),
                "popups_confirmed_count": len(popups_confirmed),
                "popups_remaining": [str(x.get("title", "")) for x in blocking_popup_entries],
                "popups_remaining_count": len(blocking_popup_entries),
                "final_blocking_titles": [str(x.get("title", "")) for x in blocking_popup_entries],
                "click_offsets_used": click_meta,
                **display_diag,
            },
        }
        _write_json(evidence_dir / "health.json", health)
        _write_text(evidence_dir / "commands.txt", "\n".join(commands_log) + "\n")
        _write_text(
            evidence_dir / "report.md",
            "# TWS X11 Autologin Report\n\n- status: FAIL\n- reason: LOGIN_NOT_CONFIRMED\n- action: verify title substrings or login tab strategy in config\n",
        )
        _sha_manifest(evidence_dir)
        print(json.dumps({"evidence_dir": str(evidence_dir), "code": EXIT_LOGIN_FAILED, "reason": fail_reason}, sort_keys=True))
        return EXIT_LOGIN_FAILED

    _screenshot(root_env, shots_dir / "final_root.png", commands_log)
    _screenshot(root_env, shots_dir / "post_login_root.png", commands_log)
    health = {
        "ts_utc": _utc_iso(),
        "ok": True,
        "code": EXIT_OK,
        "reason": "OK",
        "details": {
            "tws_pid": int(tws_pid),
            "main_window_found": bool(main_win),
            "login_window_present": bool(login_win),
            "login_window_title_seen": login_title_seen,
            "main_window_title_seen": main_title_seen,
            "api_check_host": cfg.api_host,
            "api_check_port": int(cfg.api_port),
            "api_check_result": api_check_result,
            "api_check_last_error": api_check_last_error,
            "tws_pid": tws_pid,
            "state_trace": state_trace[-200:],
            "popups_closed": popups_closed,
            "popups_confirmed": popups_confirmed,
            "popups_closed_count": len(popups_closed),
            "popups_confirmed_count": len(popups_confirmed),
            "popups_blocking_final": [],
            "popups_remaining": [],
            "popups_remaining_count": 0,
            "final_blocking_titles": [],
            "click_offsets_used": click_meta,
            "evidence_dir": str(evidence_dir),
            **display_diag,
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
