#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

SERVICE_NAME = "octa_ibkr_autologin_watch"
MISCONFIG_BACKOFF_SEC = int(os.environ.get("OCTA_AUTOLOGIN_BACKOFF_SEC", "15"))
LOOP_SLEEP_SEC = float(os.environ.get("OCTA_AUTOLOGIN_LOOP_SEC", "2"))
ACTION_COOLDOWN_SEC = float(os.environ.get("OCTA_AUTOLOGIN_ACTION_COOLDOWN_SEC", "8"))
IDLE_EVENT_EVERY = int(os.environ.get("OCTA_AUTOLOGIN_IDLE_EVENT_EVERY", "10"))
NO_WINDOW_BACKOFF_MAX_SEC = float(os.environ.get("OCTA_AUTOLOGIN_NO_WINDOW_BACKOFF_MAX_SEC", "8"))
ACTION_RETRY_GRACE_SEC = float(os.environ.get("OCTA_AUTOLOGIN_ACTION_RETRY_GRACE_SEC", "5"))
ACTION_RETRY_MAX = int(os.environ.get("OCTA_AUTOLOGIN_ACTION_RETRY_MAX", "3"))
MONITOR_POLL_SEC = float(os.environ.get("OCTA_AUTOLOGIN_MONITOR_POLL_SEC", "0.8"))
MONITOR_ACTION_RETRY_SEC = float(os.environ.get("OCTA_AUTOLOGIN_MONITOR_ACTION_RETRY_SEC", "15"))
IBKR_CLASS_ALLOWLIST = (
    "ib",
    "jts",
    "tws",
    "ibgateway",
    "traderworkstation",
    "trader workstation",
    "install4j-jclient-launcher",
    "sun-awt-x11-xdialogpeer",
    "sun-awt-x11-xframepeer",
)
DEBUG_MODE = os.environ.get("OCTA_AUTOLOGIN_DEBUG", "0") == "1"
STAGE2_KEYWORDS = (
    "verification",
    "sicherheits",
    "code",
    "two-factor",
    "challenge",
    "security",
    "ib key",
    "authenticator",
)
DISCLAIMER_KEYWORDS = (
    "disclaimer",
    "haftungsausschluss",
    "haftung",
    "terms",
    "bedingungen",
    "nutzungsbedingungen",
    "allgemeine gesch",
    "vereinbarung",
    "zustimmen",
    "akzeptieren",
    "weiter",
    "fortfahren",
    "risk disclosure",
    "risk warning",
    "risikohinweis",
    "risikowarnung",
    "risk acknowledgement",
    "trading risks",
    "ibkr disclaimer",
    "interactive brokers llc disclaimer",
    "agreements",
    "agreement",
    "customer agreement",
    "client agreement",
    "electronic disclosure",
    "electronic trading",
    "electronic communication",
    "accept",
    "continue",
)
POPUP_KEYWORDS = (
    "important",
    "update",
    "notice",
    "message",
    "alert",
    "warning",
    "confirm",
    "confirmation",
)
DISCLAIMER_TITLE_RX = re.compile(
    r"(disclaimer|haftungsausschluss|risk disclosure|risikohinweis|agreement|vereinbarung|terms|bedingungen)",
    re.IGNORECASE,
)
LOGIN_MESSAGE_TITLE_RX = re.compile(
    r"(login messages?|login message|anmeldemeldungen|anmelde.?meldungen|nachrichten bei anmeldung)",
    re.IGNORECASE,
)
ACTION_ROLE_ALLOWLIST = {"login", "disclaimer", "login_message_popup"}
MAIN_WINDOW_KEYWORDS = (
    "trader workstation",
    "mosaic",
    "classic tws",
    "classic",
    "portfolio",
    "monitor",
    "chart",
)


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def json_escape(s: str) -> str:
    return (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def redact_value(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 2:
        return "*" * len(value)
    return value[:2] + "***"


def redact_dbus(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 24:
        return value
    return value[:24] + "...[redacted]"


class Evidence:
    def __init__(self, repo: Path):
        runtime_ptr = repo / "octa/var/runtime/systemd_boot_dir"
        boot_dir = ""
        if runtime_ptr.exists():
            try:
                boot_dir = runtime_ptr.read_text(encoding="utf-8").strip()
            except Exception:
                boot_dir = ""
        if boot_dir:
            self.base_dir = Path(boot_dir) / SERVICE_NAME
        else:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            self.base_dir = repo / "octa/var/evidence" / f"{SERVICE_NAME}_{stamp}"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.run_json = self.base_dir / "run.json"
        self.events_jsonl = self.base_dir / "events.jsonl"
        self.stdout_log = self.base_dir / "stdout.log"
        self.stderr_log = self.base_dir / "stderr.log"
        self.stdout_log.touch(exist_ok=True)
        self.stderr_log.touch(exist_ok=True)

    def log_out(self, msg: str) -> None:
        line = f"[{utc_ts()}] {msg}\n"
        with self.stdout_log.open("a", encoding="utf-8") as fh:
            fh.write(line)

    def log_err(self, msg: str) -> None:
        line = f"[{utc_ts()}] {msg}\n"
        with self.stderr_log.open("a", encoding="utf-8") as fh:
            fh.write(line)

    def event(self, event_type: str, **fields: object) -> None:
        payload = {"ts": utc_ts(), "event_type": event_type, "service": SERVICE_NAME}
        payload.update(fields)
        with self.events_jsonl.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=True) + "\n")

    def write_run(self, status: str, display: str, xauthority: str, reason: str, tws_pid: str = "") -> None:
        payload = {
            "service": SERVICE_NAME,
            "status": status,
            "ts": utc_ts(),
            "reason": reason,
            "display": display,
            "xauthority": xauthority,
            "pid": tws_pid,
            "evidence_dir": str(self.base_dir),
            "env": {
                "DISPLAY": display,
                "XAUTHORITY": xauthority,
                "XDG_RUNTIME_DIR": os.environ.get("XDG_RUNTIME_DIR", ""),
                "WAYLAND_DISPLAY": os.environ.get("WAYLAND_DISPLAY", ""),
                "DBUS_SESSION_BUS_ADDRESS": redact_dbus(os.environ.get("DBUS_SESSION_BUS_ADDRESS", "")),
            },
        }
        self.run_json.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def run_cmd(ev: Evidence, cmd: List[str], timeout: int = 10) -> subprocess.CompletedProcess:
    cmd_for_log = list(cmd)
    if len(cmd_for_log) >= 2 and cmd_for_log[0] == "xdotool" and cmd_for_log[1] == "type":
        cmd_for_log[-1] = "[redacted]"
    ev.log_out("cmd=" + shlex.join(cmd_for_log))
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
    except Exception as exc:
        ev.log_err(f"cmd_exception={type(exc).__name__}:{exc}")
        return subprocess.CompletedProcess(cmd, returncode=1, stdout="", stderr=str(exc))
    if proc.stdout:
        out = proc.stdout.strip()
        if len(out) > 1200:
            out = out[:1200] + "...[truncated]"
        ev.log_out("stdout=" + out)
    if proc.stderr:
        err = proc.stderr.strip()
        if len(err) > 1200:
            err = err[:1200] + "...[truncated]"
        ev.log_err("stderr=" + err)
    return proc


def assert_tooling(ev: Evidence) -> bool:
    required = ("xdotool", "xprop")
    missing = []
    for tool in required:
        if not shutil_which(tool):
            missing.append(tool)
    if missing:
        for tool in missing:
            ev.event("autologin_error", reason=f"{tool}_missing")
        return False
    return True


def shutil_which(cmd: str) -> Optional[str]:
    probe = subprocess.run(["/usr/bin/env", "bash", "-lc", f"command -v {shlex.quote(cmd)}"], capture_output=True, text=True, check=False)
    out = (probe.stdout or "").strip()
    return out or None


def normalized_title(title: str) -> str:
    return re.sub(r"\s+", " ", (title or "").strip().lower())


def window_fingerprint(ev: Evidence, win: Dict[str, str]) -> str:
    wid = win.get("id", "")
    geom = get_xwininfo_geometry_numbers(ev, wid)
    geom_s = f"{geom.get('x','?')},{geom.get('y','?')},{geom.get('width','?')},{geom.get('height','?')}"
    return f"{(win.get('class') or '').lower()}|{normalized_title(win.get('title',''))}|{geom_s}"


def cmdline_hash_for_pid(pid: int) -> str:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", b" ").strip()
        if not raw:
            return ""
        return hashlib.sha256(raw).hexdigest()
    except Exception:
        return ""


def parse_env_strict(path: Path, ev: Evidence, file_label: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    text = path.read_text(encoding="utf-8")
    for idx, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
        if not m:
            preview = re.sub(r"[A-Za-z0-9]", "x", raw[:120])
            ev.event("env_invalid_line", file=file_label, line=idx, preview=preview)
            raise RuntimeError(f"invalid env line in {file_label}:{idx}")
        key, value = m.group(1), m.group(2)
        out[key] = value
    return out


def load_env_and_secrets(repo: Path, ev: Evidence) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    env_file = Path.home() / ".config/octa/env"
    if env_file.exists():
        perms = oct(env_file.stat().st_mode & 0o777)[2:]
        if perms not in {"400", "600"}:
            raise RuntimeError(f"env_file_insecure_permissions mode={perms}")
        merged.update(parse_env_strict(env_file, ev, str(env_file)))

    secrets_file = Path.home() / ".config/octa/ibkr_secrets.env"
    if not secrets_file.exists():
        raise RuntimeError("missing_secrets_file")
    perms = oct(secrets_file.stat().st_mode & 0o777)[2:]
    if perms not in {"400", "600"}:
        raise RuntimeError(f"secrets_file_insecure_permissions mode={perms}")

    secrets = parse_env_strict(secrets_file, ev, str(secrets_file))
    if not secrets.get("IBKR_USERNAME") or not secrets.get("IBKR_PASSWORD"):
        raise RuntimeError("missing_credentials")
    merged.update(secrets)
    return merged


def assert_x11(display: str, xauthority: str, ev: Evidence) -> bool:
    if not display:
        ev.event("x11_not_reachable", detail="display_missing")
        return False
    if not xauthority or not Path(xauthority).exists() or not os.access(xauthority, os.R_OK):
        ev.event("x11_not_reachable", detail=f"xauthority_missing_or_unreadable path={xauthority}")
        return False

    if run_cmd(ev, ["xdpyinfo", "-display", display], timeout=8).returncode != 0:
        ev.event("x11_not_reachable", detail=f"xdpyinfo_failed display={display}")
        return False
    if run_cmd(ev, ["xset", "-display", display, "q"], timeout=8).returncode != 0:
        ev.event("x11_not_reachable", detail=f"xset_q_failed display={display}")
        return False
    ev.event("x11_checks_ok", display=display, xauthority=xauthority)
    return True


def list_windows(ev: Evidence) -> List[Dict[str, str]]:
    windows: List[Dict[str, str]] = []
    seen_ids = set()

    xdotool_patterns = ["jts", "ibkr", "tws", "Trader", "IB Gateway"]
    for pattern in xdotool_patterns:
        probe = run_cmd(ev, ["xdotool", "search", "--onlyvisible", "--class", pattern], timeout=6)
        if probe.returncode != 0 or not probe.stdout.strip():
            continue
        for wid in probe.stdout.splitlines():
            wid = wid.strip()
            if not wid or wid in seen_ids:
                continue
            info = run_cmd(ev, ["xprop", "-id", wid, "WM_NAME", "_NET_WM_NAME", "WM_CLASS", "WM_WINDOW_ROLE"], timeout=6)
            if info.returncode != 0:
                continue
            title = ""
            wm_class = ""
            role = ""
            for line in info.stdout.splitlines():
                if line.startswith("WM_NAME") or line.startswith("_NET_WM_NAME"):
                    title = line.split("=", 1)[-1].strip().strip('"')
                if line.startswith("WM_CLASS"):
                    wm_class = line.split("=", 1)[-1].strip().strip('"')
                if line.startswith("WM_WINDOW_ROLE"):
                    role = line.split("=", 1)[-1].strip().strip('"')
            windows.append({"id": wid, "title": title, "class": wm_class, "role": role})
            seen_ids.add(wid)

    if windows:
        return windows

    root = run_cmd(ev, ["xprop", "-root", "_NET_CLIENT_LIST"], timeout=8)
    if root.returncode == 0 and "#" in root.stdout:
        ids = [w.strip() for w in root.stdout.split("#", 1)[1].replace(",", " ").split() if w.strip()]
        for wid in ids:
            if wid in seen_ids:
                continue
            info = run_cmd(ev, ["xprop", "-id", wid, "WM_NAME", "_NET_WM_NAME", "WM_CLASS", "WM_WINDOW_ROLE"], timeout=8)
            if info.returncode != 0:
                continue
            title = ""
            wm_class = ""
            role = ""
            for line in info.stdout.splitlines():
                if line.startswith("WM_NAME") or line.startswith("_NET_WM_NAME"):
                    title = line.split("=", 1)[-1].strip().strip('"')
                if line.startswith("WM_CLASS"):
                    wm_class = line.split("=", 1)[-1].strip().strip('"')
                if line.startswith("WM_WINDOW_ROLE"):
                    role = line.split("=", 1)[-1].strip().strip('"')
            windows.append({"id": wid, "title": title, "class": wm_class, "role": role})
            seen_ids.add(wid)
        if windows:
            return windows

    tree = run_cmd(ev, ["xwininfo", "-root", "-tree"], timeout=8)
    if tree.returncode != 0:
        return windows
    rx = re.compile(r"\s*(0x[0-9a-fA-F]+)\s+\"([^\"]*)\"")
    for line in tree.stdout.splitlines():
        m = rx.match(line)
        if m:
            wid = m.group(1)
            if wid in seen_ids:
                continue
            windows.append({"id": wid, "title": m.group(2), "class": "", "role": ""})
            seen_ids.add(wid)
    return windows


def fallback_template_match(ev: Evidence) -> Optional[str]:
    try:
        import cv2  # type: ignore
        import numpy as np  # type: ignore
    except Exception:
        ev.event("template_fallback_unavailable", detail="opencv_not_installed")
        return None

    screenshot = run_cmd(ev, ["import", "-window", "root", "png:-"], timeout=10)
    if screenshot.returncode != 0 or not screenshot.stdout:
        ev.event("template_fallback_failed", detail="root_capture_failed")
        return None

    ev.event("template_fallback_checked", detail="opencv_loaded_but_no_templates_configured")
    _ = cv2, np
    return None


def get_xwininfo_geometry(ev: Evidence, wid: str) -> str:
    info = run_cmd(ev, ["xwininfo", "-id", wid], timeout=6)
    if info.returncode != 0:
        return ""
    keep: List[str] = []
    for line in info.stdout.splitlines():
        s = line.strip()
        if s.startswith(("Absolute upper-left X:", "Absolute upper-left Y:", "Width:", "Height:", "Map State:", "Override Redirect State:")):
            keep.append(s)
    return "; ".join(keep[:8])


def get_xwininfo_geometry_numbers(ev: Evidence, wid: str) -> Dict[str, int]:
    info = run_cmd(ev, ["xwininfo", "-id", wid], timeout=6)
    if info.returncode != 0:
        return {}
    out: Dict[str, int] = {}
    patterns = {
        "x": r"Absolute upper-left X:\s*(-?\d+)",
        "y": r"Absolute upper-left Y:\s*(-?\d+)",
        "width": r"Width:\s*(\d+)",
        "height": r"Height:\s*(\d+)",
    }
    for key, rx in patterns.items():
        m = re.search(rx, info.stdout)
        if m:
            out[key] = int(m.group(1))
    return out


def classify_window(win: Dict[str, str]) -> Dict[str, object]:
    title = (win.get("title") or "").lower()
    klass = (win.get("class") or "").lower()
    wm_role = (win.get("role") or "").lower()
    text = f"{title} {klass} {wm_role}"
    score = 0
    reasons: List[str] = []
    if any(tok in klass for tok in IBKR_CLASS_ALLOWLIST):
        score += 5
        reasons.append("ibkr_class")
    if any(tok in text for tok in ("trader workstation", "interactive brokers", "ibkr", "jts", "tws", "install4j-jclient-launcher")):
        score += 3
        reasons.append("ibkr_text")

    login_hits = [tok for tok in ("login", "log in", "authenticate", "authentication", "username", "password", "anmelden", "sign in") if tok in text]
    disclaimer_hits = [tok for tok in DISCLAIMER_KEYWORDS if tok in text]
    disclaimer_title_match = bool(DISCLAIMER_TITLE_RX.search(title))
    popup_hits = [tok for tok in POPUP_KEYWORDS if tok in text]
    main_hits = [tok for tok in MAIN_WINDOW_KEYWORDS if tok in text]
    stage2_hits = [tok for tok in STAGE2_KEYWORDS if tok in text]
    if login_hits:
        score += 4
        reasons.append("login_hint")
    if disclaimer_hits:
        score += 4
        reasons.append("disclaimer_hint")
    if disclaimer_title_match:
        score += 4
        reasons.append("disclaimer_title")
    if stage2_hits:
        score += 5
        reasons.append("stage2_hint")
    if popup_hits:
        score += 3
        reasons.append("popup_hint")
    if main_hits:
        score += 3
        reasons.append("main_hint")

    role = "other"
    if stage2_hits:
        role = "stage2"
    elif disclaimer_hits or disclaimer_title_match:
        role = "disclaimer"
    elif popup_hits and not login_hits:
        role = "popup_modal"
    elif login_hits:
        role = "login"
    elif main_hits:
        role = "main"
    elif score >= 6:
        role = "login"
        reasons.append("ibkr_default_login")
    return {"role": role, "score": score, "reasons": reasons}


def is_ibkr_class_allowed(win: Dict[str, str]) -> bool:
    klass = (win.get("class") or "").lower()
    return any(tok in klass for tok in IBKR_CLASS_ALLOWLIST)


def select_candidates(
    ev: Evidence, windows: List[Dict[str, str]], main_window_seen: bool, login_submit_recent: bool
) -> Tuple[
    List[Dict[str, str]],
    List[Dict[str, str]],
    List[Dict[str, str]],
    List[Dict[str, str]],
    List[Dict[str, str]],
    List[Dict[str, str]],
]:
    login_hits: List[Dict[str, str]] = []
    disclaimer_hits: List[Dict[str, str]] = []
    stage2_hits: List[Dict[str, str]] = []
    popup_hits: List[Dict[str, str]] = []
    login_message_hits: List[Dict[str, str]] = []
    main_hits: List[Dict[str, str]] = []
    for w in windows:
        cls = classify_window(w)
        role = str(cls["role"])
        score = int(cls["score"])
        reasons = list(cls["reasons"])
        class_allowed = is_ibkr_class_allowed(w)
        title = w.get("title", "") or ""
        if class_allowed and LOGIN_MESSAGE_TITLE_RX.search(title) and (main_window_seen or login_submit_recent):
            role = "login_message_popup"
            score = max(score, 6)
            reasons.append("login_message_context")
        is_action_phase = role in {"login", "disclaimer", "login_message_popup", "stage2"}

        # Strict action allowlist: no action phase without IBKR class allowlist match.
        if is_action_phase and not class_allowed:
            ev.event(
                "unknown_window_ignored",
                wid=w.get("id", ""),
                title=w.get("title", ""),
                wm_class=w.get("class", ""),
                reason="not_allowlisted",
            )
            continue

        if score <= 0:
            continue
        w["score"] = str(score)
        w["candidate_role"] = role
        w["reasons"] = ",".join(reasons)
        if role not in ACTION_ROLE_ALLOWLIST.union({"main", "stage2"}):
            ev.event(
                "unknown_window_ignored",
                wid=w.get("id", ""),
                title=w.get("title", ""),
                wm_class=w.get("class", ""),
                reason=f"unsupported_role:{role}",
            )
            continue
        if role == "disclaimer" and score >= 6:
            disclaimer_hits.append(w)
        elif role == "stage2" and score >= 6:
            stage2_hits.append(w)
        elif role == "login_message_popup" and score >= 6:
            login_message_hits.append(w)
        elif role == "main" and score >= 5 and class_allowed:
            main_hits.append(w)
        elif role == "login" and score >= 6:
            login_hits.append(w)
    login_hits.sort(key=lambda x: int(x.get("score", "0")), reverse=True)
    disclaimer_hits.sort(key=lambda x: int(x.get("score", "0")), reverse=True)
    stage2_hits.sort(key=lambda x: int(x.get("score", "0")), reverse=True)
    login_message_hits.sort(key=lambda x: int(x.get("score", "0")), reverse=True)
    popup_hits.sort(key=lambda x: int(x.get("score", "0")), reverse=True)
    main_hits.sort(key=lambda x: int(x.get("score", "0")), reverse=True)
    return login_hits, disclaimer_hits, stage2_hits, login_message_hits, popup_hits, main_hits


def focus_window(ev: Evidence, wid: str) -> bool:
    seq = [
        ["xdotool", "windowactivate", "--sync", wid],
        ["xdotool", "windowraise", wid],
        ["xdotool", "windowfocus", "--sync", wid],
    ]
    for cmd in seq:
        if run_cmd(ev, cmd, timeout=6).returncode != 0:
            ev.event("focus_applied", wid=wid, ok=False)
            return False
    time.sleep(0.15)
    ev.event("focus_applied", wid=wid, ok=True)
    return True


def ibkr_java_pids(ev: Evidence) -> List[int]:
    uid = str(os.getuid())
    probe = run_cmd(ev, ["pgrep", "-u", uid, "-fa", "java"], timeout=6)
    if probe.returncode != 0 or not probe.stdout.strip():
        return []
    out: List[int] = []
    for line in probe.stdout.splitlines():
        low = line.lower()
        if "/jts/" not in low and "trader workstation" not in low and "ibgateway" not in low:
            continue
        parts = line.strip().split(maxsplit=1)
        if not parts:
            continue
        try:
            out.append(int(parts[0]))
        except ValueError:
            continue
    return sorted(set(out))


def tws_process_running(ev: Evidence) -> bool:
    return len(ibkr_java_pids(ev)) >= 1


def debug_snapshot(ev: Evidence, windows: List[Dict[str, str]]) -> None:
    if not DEBUG_MODE:
        return
    focus = run_cmd(ev, ["xdotool", "getwindowfocus"], timeout=4)
    focus_wid = (focus.stdout or "").strip() if focus.returncode == 0 else ""
    visible = run_cmd(ev, ["xdotool", "search", "--onlyvisible", "--name", ".*"], timeout=6)
    visible_ids = [x.strip() for x in (visible.stdout or "").splitlines() if x.strip()][:20]
    ev.event("debug_snapshot", focused_wid=focus_wid, visible_count=len(visible_ids), visible_ids=visible_ids)


def login_or_disclaimer_gone(ev: Evidence, target_wid: str, expected_role: str, old_title: str) -> Tuple[bool, str]:
    deadline = time.time() + ACTION_RETRY_GRACE_SEC
    while time.time() < deadline:
        windows = list_windows(ev)
        login_hits, disclaimer_hits, stage2_hits, login_message_hits, popup_hits, main_hits = select_candidates(
            ev, windows, main_window_seen=True, login_submit_recent=True
        )
        if expected_role == "disclaimer":
            target_list = disclaimer_hits
        elif expected_role == "popup_modal":
            target_list = popup_hits
        elif expected_role == "login_message_popup":
            target_list = login_message_hits
        else:
            target_list = login_hits
        still = next((w for w in target_list if w.get("id") == target_wid), None)
        if still is None:
            return True, "target_window_gone"
        if (still.get("title") or "") != old_title:
            return True, "target_title_changed"
        if stage2_hits:
            return True, "stage2_detected"
        if expected_role == "login" and disclaimer_hits:
            return True, "disclaimer_after_login"
        if expected_role in {"login", "disclaimer", "popup_modal", "login_message_popup"} and main_hits:
            return True, "main_window_visible"
        if expected_role == "login":
            for w in windows:
                role = str(classify_window(w).get("role", "other"))
                if role == "other":
                    text = f"{w.get('title', '')} {w.get('class', '')}".lower()
                    if any(tok in text for tok in ("trader workstation", "tws", "jts", "mosaic", "portfolio")):
                        return True, "post_login_window_seen"
        time.sleep(0.5)
    return False, "no_state_change"


def target_window_disappeared(ev: Evidence, target_wid: str, expected_role: str, timeout_sec: float = 3.0) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        windows = list_windows(ev)
        login_hits, disclaimer_hits, _, login_message_hits, _, _ = select_candidates(
            ev, windows, main_window_seen=True, login_submit_recent=True
        )
        if expected_role == "disclaimer":
            target_list = disclaimer_hits
        elif expected_role == "login_message_popup":
            target_list = login_message_hits
        else:
            target_list = login_hits
        if not any(w.get("id") == target_wid for w in target_list):
            return True
        time.sleep(0.6)
    return False


def apply_username_focus_click(ev: Evidence, wid: str, y_ratio: float) -> bool:
    geom = get_xwininfo_geometry_numbers(ev, wid)
    if not all(k in geom for k in ("x", "y", "width", "height")):
        return False
    click_x = int(geom["x"] + int(geom["width"] * 0.30))
    click_y = int(geom["y"] + int(geom["height"] * y_ratio))
    rc = run_cmd(ev, ["xdotool", "mousemove", "--sync", str(click_x), str(click_y)], timeout=6).returncode
    if rc != 0:
        return False
    rc = run_cmd(ev, ["xdotool", "click", "1"], timeout=6).returncode
    ev.event("action_click", wid=wid, x=click_x, y=click_y, button=1, role="login")
    return rc == 0


def geometry_click_ladder(ev: Evidence, wid: str, role: str) -> bool:
    geom = get_xwininfo_geometry_numbers(ev, wid)
    if not all(k in geom for k in ("x", "y", "width", "height")):
        return False
    points = [(0.85, 0.92), (0.70, 0.92), (0.85, 0.85)]
    ok_any = False
    for idx, (rx, ry) in enumerate(points, start=1):
        click_x = int(geom["x"] + int(geom["width"] * rx))
        click_y = int(geom["y"] + int(geom["height"] * ry))
        rc_move = run_cmd(ev, ["xdotool", "mousemove", "--sync", str(click_x), str(click_y)], timeout=6).returncode
        rc_click = run_cmd(ev, ["xdotool", "click", "1"], timeout=6).returncode if rc_move == 0 else 1
        ev.event("action_click", wid=wid, x=click_x, y=click_y, button=1, role=role, ladder_index=idx)
        if rc_click == 0:
            ok_any = True
        time.sleep(0.15)
    return ok_any


def resolve_ok_close_click_points(ev: Evidence, wid: str) -> List[Tuple[int, int]]:
    tree = run_cmd(ev, ["xwininfo", "-id", wid, "-tree"], timeout=8)
    if tree.returncode != 0:
        return []
    lines = (tree.stdout or "").splitlines()
    points: List[Tuple[int, int]] = []
    labels = ("ok", "close", "continue", "accept", "agree")
    child_ids: List[str] = []
    for line in lines:
        m = re.search(r"(0x[0-9a-fA-F]+)", line)
        if not m:
            continue
        child_id = m.group(1)
        if child_id.lower() == wid.lower():
            continue
        low = line.lower()
        if any(tok in low for tok in labels):
            child_ids.append(child_id)
    for cid in child_ids[:5]:
        g = get_xwininfo_geometry_numbers(ev, cid)
        if not all(k in g for k in ("x", "y", "width", "height")):
            continue
        cx = int(g["x"] + int(g["width"] / 2))
        cy = int(g["y"] + int(g["height"] / 2))
        if (cx, cy) not in points:
            points.append((cx, cy))
        if len(points) >= 2:
            break
    return points


def handle_disclaimer(ev: Evidence, win: Dict[str, str]) -> bool:
    wid = win["id"]
    title = win.get("title", "")
    wm_class = win.get("class", "")
    score = int(win.get("score", "0"))
    reasons = win.get("reasons", "")
    geom = get_xwininfo_geometry(ev, wid)
    ev.event(
        "disclaimer_detected",
        role="disclaimer",
        wid=wid,
        wm_class=wm_class,
        title=title,
        score=score,
        reasons=reasons,
        geometry=geom,
    )
    ev.event(
        "window_detected",
        role="disclaimer",
        wid=wid,
        wm_class=wm_class,
        title=title,
        score=score,
        reasons=reasons,
        geometry=geom,
    )
    if not focus_window(ev, wid):
        ev.event("autologin_error", reason="focus_failed_disclaimer", wid=wid)
        ev.event("diagnosis", reason="disclaimer_window_found_but_focus_failed", wid=wid)
        return False

    # Deterministic first attempt: relative click at 80% width / 90% height.
    geom_numbers = get_xwininfo_geometry_numbers(ev, wid)
    if all(k in geom_numbers for k in ("x", "y", "width", "height")):
        click_x = int(geom_numbers["x"] + int(geom_numbers["width"] * 0.80))
        click_y = int(geom_numbers["y"] + int(geom_numbers["height"] * 0.90))
        ev.event("popup_close_attempt", role="disclaimer", step="relative_80_90_click", method="click_relative", wid=wid)
        rc_move = run_cmd(ev, ["xdotool", "mousemove", "--sync", str(click_x), str(click_y)], timeout=6).returncode
        rc_click = run_cmd(ev, ["xdotool", "click", "1"], timeout=6).returncode if rc_move == 0 else 1
        if rc_click == 0:
            ev.event("action_click", wid=wid, x=click_x, y=click_y, button=1, role="disclaimer", ladder_index=1)
            if target_window_disappeared(ev, wid, "disclaimer", timeout_sec=3.0):
                ev.event("disclaimer_action_done", wid=wid, method="click_relative_80_90", ok=True, reason="target_window_gone")
                ev.event("state_transition", **{"from": "B_DISCLAIMER", "to": "C_STEADY"}, reason="target_window_gone")
                ev.event("popup_closed_ok", role="disclaimer", wid=wid, reason="target_window_gone")
                return True
    else:
        ev.event("popup_close_attempt", role="disclaimer", step="relative_80_90_click", method="click_relative", wid=wid, reason="geometry_missing")

    ev.event("disclaimer_action_start", wid=wid, method="keys")
    key_ladders = (4, 6, 8)
    for idx, tab_count in enumerate(key_ladders, start=1):
        ev.event("popup_close_attempt", role="disclaimer", step=f"tab{tab_count}_return", method="keys", wid=wid)
        ok = True
        for _ in range(tab_count):
            rc = run_cmd(ev, ["xdotool", "key", "--clearmodifiers", "Tab"], timeout=6).returncode
            ev.event("action_key", key="Tab", wid=wid, role="disclaimer", ladder_index=idx)
            if rc != 0:
                ok = False
                break
            time.sleep(0.12)
        if not ok:
            continue
        rc_return = run_cmd(ev, ["xdotool", "key", "--clearmodifiers", "Return"], timeout=6).returncode
        ev.event("action_key", key="Return", wid=wid, role="disclaimer", ladder_index=idx)
        if rc_return != 0:
            continue
        if target_window_disappeared(ev, wid, "disclaimer", timeout_sec=3.0):
            ev.event("disclaimer_action_done", wid=wid, method="tab_return_ladder", ok=True, reason="target_window_gone")
            ev.event("state_transition", **{"from": "B_DISCLAIMER", "to": "C_STEADY"}, reason="target_window_gone")
            ev.event("popup_closed_ok", role="disclaimer", wid=wid, reason="target_window_gone")
            return True

    ev.event("disclaimer_action_done", wid=wid, method="tab_return_ladder", ok=False, reason="disclaimer_persisted_after_actions")
    ev.event("diagnosis", reason="disclaimer_persisted_after_actions", wid=wid)
    return False


def handle_popup_modal(ev: Evidence, win: Dict[str, str]) -> bool:
    wid = win["id"]
    title = win.get("title", "")
    wm_class = win.get("class", "")
    score = int(win.get("score", "0"))
    reasons = win.get("reasons", "")
    geom = get_xwininfo_geometry(ev, wid)
    ev.event(
        "popup_detected",
        wid=wid,
        wm_class=wm_class,
        title=title,
        role="popup_modal",
        score=score,
        reasons=reasons,
        geometry=geom,
    )
    if not focus_window(ev, wid):
        ev.event("autologin_error", reason="focus_failed_popup", wid=wid)
        return False

    ladders = [
        ("escape", ["xdotool", "key", "--clearmodifiers", "Escape"]),
        ("alt_f4", ["xdotool", "key", "--clearmodifiers", "alt+F4"]),
    ]
    for step, cmd in ladders:
        ev.event("popup_close_attempt", role="popup_modal", wid=wid, step=step, method="keys")
        rc = run_cmd(ev, cmd, timeout=6).returncode
        if cmd[1] == "key":
            ev.event("action_key", key=cmd[-1], wid=wid, role="popup_modal")
        if rc == 0:
            changed, reason = login_or_disclaimer_gone(ev, wid, "popup_modal", title)
            if changed:
                ev.event("popup_closed_ok", role="popup_modal", wid=wid, reason=reason)
                ev.event("state_transition", **{"from": "B_POPUP", "to": "C_STEADY"}, reason=reason)
                return True

    ev.event("popup_close_attempt", role="popup_modal", wid=wid, step="geometry_clicks", method="clicks")
    click_ok = geometry_click_ladder(ev, wid, "popup_modal")
    if click_ok:
        changed, reason = login_or_disclaimer_gone(ev, wid, "popup_modal", title)
        if changed:
            ev.event("popup_closed_ok", role="popup_modal", wid=wid, reason=reason)
            ev.event("state_transition", **{"from": "B_POPUP", "to": "C_STEADY"}, reason=reason)
            return True
    return False


def handle_login_message_popup(ev: Evidence, win: Dict[str, str]) -> bool:
    wid = win["id"]
    title = win.get("title", "")
    wm_class = win.get("class", "")
    score = int(win.get("score", "0"))
    reasons = win.get("reasons", "")
    geom = get_xwininfo_geometry(ev, wid)
    ev.event(
        "window_detected",
        wid=wid,
        wm_class=wm_class,
        title=title,
        role="login_message_popup",
        score=score,
        reasons=reasons,
        geometry=geom,
    )
    if not focus_window(ev, wid):
        ev.event("autologin_error", reason="focus_failed_login_message_popup", wid=wid)
        ev.event("popup_closed", role="login_message_popup", wid=wid, success=False, reason="focus_failed")
        return False

    ev.event("popup_close_attempt", role="login_message_popup", wid=wid, step="alt_f4", method="keys")
    rc = run_cmd(ev, ["xdotool", "key", "--clearmodifiers", "alt+F4"], timeout=6).returncode
    ev.event("action_key", key="alt+F4", wid=wid, role="login_message_popup")
    if rc == 0 and target_window_disappeared(ev, wid, "login_message_popup", timeout_sec=3.0):
        ev.event("popup_closed", role="login_message_popup", wid=wid, success=True, reason="target_window_gone")
        return True

    ev.event("popup_closed", role="login_message_popup", wid=wid, success=False, reason="login_messages_persisted_after_alt_f4")
    ev.event("diagnosis", reason="login_messages_persisted_after_alt_f4", wid=wid)
    return False


def handle_login(ev: Evidence, win: Dict[str, str], username: str, password: str) -> Tuple[bool, bool]:
    wid = win["id"]
    title = win.get("title", "")
    wm_class = win.get("class", "")
    score = int(win.get("score", "0"))
    reasons = win.get("reasons", "")
    geom = get_xwininfo_geometry(ev, wid)
    ev.event(
        "window_detected",
        role="login",
        wid=wid,
        wm_class=wm_class,
        title=title,
        score=score,
        reasons=reasons,
        geometry=geom,
    )
    if not focus_window(ev, wid):
        ev.event("autologin_error", reason="focus_failed_login", wid=wid)
        ev.event("diagnosis", reason="login_window_found_but_focus_failed", wid=wid)
        return False, False

    ev.event("action_type", field="username", length=len(username), redacted="yes", wid=wid)
    ev.event("action_type", field="password", length=len(password), redacted="yes", wid=wid)

    ladder_steps = [
        ("focus_click", {"tabs_before": 0, "shift_tabs_before": 0}),
        ("tab_x1", {"tabs_before": 1, "shift_tabs_before": 0}),
        ("tab_x2", {"tabs_before": 2, "shift_tabs_before": 0}),
        ("shift_tab_x1", {"tabs_before": 0, "shift_tabs_before": 1}),
    ]
    submitted_any = False
    for attempt_id, (ladder_step, cfg) in enumerate(ladder_steps, start=1):
        if not focus_window(ev, wid):
            ev.event("login_attempt_done", role="login", success=False, reason="focus_failed", attempt_id=attempt_id)
            continue
        ev.event("login_attempt_start", role="login", wid=wid, attempt_id=attempt_id, ladder_step=ladder_step)

        if ladder_step == "focus_click":
            apply_username_focus_click(ev, wid, 0.38)
            apply_username_focus_click(ev, wid, 0.45)

        for _ in range(int(cfg["tabs_before"])):
            run_cmd(ev, ["xdotool", "key", "--clearmodifiers", "Tab"], timeout=8)
            ev.event("action_key", key="Tab", wid=wid, role="login", attempt_id=attempt_id)
            time.sleep(0.15)
        for _ in range(int(cfg["shift_tabs_before"])):
            run_cmd(ev, ["xdotool", "key", "--clearmodifiers", "Shift+Tab"], timeout=8)
            ev.event("action_key", key="Shift+Tab", wid=wid, role="login", attempt_id=attempt_id)
            time.sleep(0.15)

        seq = [
            ("clear_user", ["xdotool", "key", "--clearmodifiers", "ctrl+a", "BackSpace"]),
            ("type_user", ["xdotool", "type", "--clearmodifiers", "--delay", "20", username]),
            ("tab_to_pass", ["xdotool", "key", "--clearmodifiers", "Tab"]),
            ("clear_pass", ["xdotool", "key", "--clearmodifiers", "ctrl+a", "BackSpace"]),
            ("type_pass", ["xdotool", "type", "--clearmodifiers", "--delay", "20", password]),
            ("submit", ["xdotool", "key", "--clearmodifiers", "Return"]),
        ]
        step_failed = ""
        for step, cmd in seq:
            rc = run_cmd(ev, cmd, timeout=8).returncode
            if cmd[1] == "key":
                ev.event("action_key", key=" ".join(cmd[3:]), wid=wid, role="login", step=step, attempt_id=attempt_id)
            if step == "submit":
                ev.event("login_submit", method="Return", wid=wid, ok=(rc == 0), attempt_id=attempt_id)
                if rc == 0:
                    submitted_any = True
            if rc != 0:
                step_failed = step
                break
            time.sleep(0.15)
        if step_failed:
            ev.event("login_attempt_done", role="login", success=False, reason=f"step_failed:{step_failed}", attempt_id=attempt_id)
            continue

        time.sleep(2.0)
        changed, reason = login_or_disclaimer_gone(ev, wid, "login", title)
        if reason == "stage2_detected":
            ev.event("stage2_detected", wid=wid, attempt_id=attempt_id)
            ev.event("login_attempt_done", role="login", success=True, state_change=reason, attempt_id=attempt_id)
            return True, submitted_any
        ev.event("login_attempt_done", role="login", success=changed, state_change=reason, attempt_id=attempt_id)
        if changed:
            return True, submitted_any
    ev.event("diagnosis", reason="login_attempted_but_window_persisted", wid=wid)
    return False, submitted_any


def enter_state(ev: Evidence, state: str) -> None:
    ev.event("state_enter", state=state)


def emit_state_change(ev: Evidence, old_state: str, new_state: str, reason: str) -> None:
    ev.event("state_transition", **{"from": old_state, "to": new_state}, reason=reason)
    ev.event("state_change", from_state=old_state, to_state=new_state, reason=reason)


def emit_done(ev: Evidence, reason: str) -> None:
    ev.event("done_latched", reason=reason)
    ev.event("done", reason=reason)
    ev.event("login_flow_complete", reason=reason)


def argv_mode_present(argv: List[str]) -> bool:
    for token in argv:
        if token == "--mode" or token.startswith("--mode="):
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="OCTA IBKR visible X11 autologin/disclaimer watcher")
    parser.add_argument("--mode", choices=["once", "monitor"], default=None, help="execution mode")
    parser.add_argument("--once", action="store_true", help="compat flag for --mode once")
    parser.add_argument("--stuck-after-sec", type=int, default=120, help="emit stuck after no state progress")
    parser.add_argument("--stuck-cooldown-sec", type=int, default=600, help="minimum seconds between repeated stuck events")
    args = parser.parse_args()
    argv_has_mode = argv_mode_present(sys.argv[1:])
    argv_mode = args.mode if argv_has_mode else None

    repo = Path(__file__).resolve().parents[1]
    ev = Evidence(repo)

    status_reason = "starting"
    display = os.environ.get("DISPLAY", "")
    xauthority = os.environ.get("XAUTHORITY", "")

    try:
        merged = load_env_and_secrets(repo, ev)
        for key, value in merged.items():
            if key not in {"IBKR_PASSWORD"}:
                os.environ[key] = value
        # secrets always in process env for action use, never logged as plain text
        os.environ["IBKR_USERNAME"] = merged["IBKR_USERNAME"]
        os.environ["IBKR_PASSWORD"] = merged["IBKR_PASSWORD"]
    except Exception as exc:
        status_reason = f"config_error:{exc}"
        ev.event("autologin_error", reason=status_reason)
        ev.write_run("error", display, xauthority, status_reason)
        time.sleep(MISCONFIG_BACKOFF_SEC)
        return 1

    if not display:
        display = os.environ.get("OCTA_DEBUG_DISPLAY", ":0")
    if not xauthority:
        xauthority = os.environ.get("OCTA_DEBUG_XAUTHORITY", "")
    os.environ["DISPLAY"] = display
    os.environ["XAUTHORITY"] = xauthority

    env_mode_raw = (os.environ.get("OCTA_AUTOLOGIN_MODE", "") or "").strip().lower()
    env_mode = env_mode_raw if env_mode_raw in {"once", "monitor"} else None
    mode_reason = ""
    mode_source = ""
    if argv_mode in {"once", "monitor"}:
        selected_mode = argv_mode
        mode_source = "argv_mode"
        mode_reason = "cli_explicit"
    elif bool(args.once):
        selected_mode = "once"
        mode_source = "argv_once_flag"
        mode_reason = "compat_once_flag"
    elif env_mode in {"once", "monitor"}:
        selected_mode = str(env_mode)
        mode_source = "env_mode"
        mode_reason = "env_default"
    else:
        selected_mode = "once"
        mode_source = "hard_default"
        mode_reason = "no_valid_mode_from_argv_or_env"
    run_once = selected_mode == "once"

    ev.event(
        "mode_selected",
        mode=selected_mode,
        argv_mode=argv_mode,
        env_mode=env_mode_raw,
        argv_has_mode=argv_has_mode,
        mode_source=mode_source,
        mode_reason=mode_reason,
        once_explanation=(mode_reason if run_once else ""),
    )
    if env_mode_raw and env_mode is None:
        ev.event("mode_env_invalid", env_mode=env_mode_raw, fallback_mode=selected_mode)

    username = os.environ.get("IBKR_USERNAME", "")
    password = os.environ.get("IBKR_PASSWORD", "")
    if not username or not password:
        status_reason = "credentials_missing_env"
        ev.event("autologin_error", reason=status_reason)
        ev.write_run("error", display, xauthority, status_reason)
        time.sleep(MISCONFIG_BACKOFF_SEC)
        return 1
    if not assert_tooling(ev):
        ev.write_run("error", display, xauthority, "missing_required_tooling")
        return 1

    ev.event("watcher_start", display=display, xauthority=xauthority, username_len=len(username), password_len=len(password), mode=selected_mode)
    ev.write_run("ok", display, xauthority, "running")

    last_action_ts = 0.0
    idle_counter = 0
    last_fallback_ts = 0.0
    action_performed = False
    no_window_backoff_sec = 1.0
    failed_action_count = 0
    done_latched = False
    main_window_seen = False
    last_login_submit_ts = 0.0
    state_epoch = 0
    once_state_action_done: Dict[str, bool] = {"S1_LOGIN": False, "S2_DISCLAIMER": False, "S3_CLOSE_POPUPS": False}
    action_gate: Dict[str, Dict[str, object]] = {
        "S1_LOGIN": {"epoch": -1, "fp": "", "ts": 0.0, "ok": False},
        "S2_DISCLAIMER": {"epoch": -1, "fp": "", "ts": 0.0, "ok": False},
        "S3_CLOSE_POPUPS": {"epoch": -1, "fp": "", "ts": 0.0, "ok": False},
    }
    last_state_change_ts = time.time()
    last_action_name = ""
    last_action_ok = False
    last_window_fingerprint = ""
    last_stuck_emit_ts = 0.0
    last_stuck_key = ""
    last_tws_marker = ""
    state = "S0_WAIT_TWS"
    enter_state(ev, state)
    done_candidate_since = 0.0

    while True:
        loop_state_before = state
        if not assert_x11(display, xauthority, ev):
            ev.event("no_action", reason="x11_not_reachable", state=state)
            if run_once:
                ev.event("idle_exit", reason="x11_not_reachable", state=state)
                return 1
            time.sleep(min(NO_WINDOW_BACKOFF_MAX_SEC, max(1.0, no_window_backoff_sec)))
            no_window_backoff_sec = min(NO_WINDOW_BACKOFF_MAX_SEC, no_window_backoff_sec * 2.0)
            continue

        windows = list_windows(ev)
        debug_snapshot(ev, windows)
        ev.event(
            "window_snapshot",
            windows=[{"wid": w.get("id", ""), "wm_class": w.get("class", ""), "title": w.get("title", ""), "role": w.get("role", "")} for w in windows[:20]],
            total_windows=len(windows),
        )
        now = time.time()
        login_hits, disclaimer_hits, stage2_hits, login_message_hits, popup_hits, main_hits = select_candidates(
            ev,
            windows,
            main_window_seen=main_window_seen,
            login_submit_recent=((now - last_login_submit_ts) <= 60.0),
        )
        if main_hits:
            main_window_seen = True

        pids = ibkr_java_pids(ev)
        if len(pids) == 1:
            pid = pids[0]
            marker = f"{pid}:{cmdline_hash_for_pid(pid)}"
            if marker != last_tws_marker:
                ev.event("tws_process", pid=pid, cmdline_hash=marker.split(":", 1)[1])
                last_tws_marker = marker
        elif len(pids) == 0:
            if last_tws_marker != "missing":
                ev.event("tws_missing", reason="no_tws_process")
                last_tws_marker = "missing"
        if len(pids) > 1:
            ev.event("multi_instance_detected", pids=pids)
            ev.write_run("error", display, xauthority, "multi_instance_detected")
            return 1

        acted = False
        if done_latched:
            ev.event("idle_noop", reason="done_latched", windows_seen=len(windows))
            if run_once:
                emit_done(ev, "done_latched")
                return 0
            time.sleep(no_window_backoff_sec)
            no_window_backoff_sec = min(NO_WINDOW_BACKOFF_MAX_SEC, no_window_backoff_sec * 2.0)
            continue

        if state == "S0_WAIT_TWS":
            if len(pids) == 1:
                emit_state_change(ev, state, "S1_LOGIN", "single_instance_detected")
                state = "S1_LOGIN"
                state_epoch += 1
                last_state_change_ts = time.time()
                enter_state(ev, state)
                no_window_backoff_sec = 1.0
            else:
                ev.event("no_action", reason="no_ibkr_windows", state=state)
                if run_once:
                    ev.event("idle_exit", reason="no_window_found", state=state)
                    return 0
                time.sleep(no_window_backoff_sec)
                no_window_backoff_sec = min(NO_WINDOW_BACKOFF_MAX_SEC, no_window_backoff_sec * 2.0)
                continue

        if state == "S1_LOGIN":
            if stage2_hits:
                top = stage2_hits[0]
                ev.event("stage2_detected", wid=top.get("id", ""), title=top.get("title", ""), wm_class=top.get("class", ""), score=int(top.get("score", "0")))
                emit_state_change(ev, state, "S4_DONE", "stage2_detected")
                state = "S4_DONE"
                state_epoch += 1
                last_state_change_ts = time.time()
                enter_state(ev, state)
                done_latched = True
                if run_once:
                    emit_done(ev, "stage2_detected")
                    return 0
                continue
            if login_hits and (now - last_action_ts) >= ACTION_COOLDOWN_SEC and (not run_once or not once_state_action_done["S1_LOGIN"]):
                fp = window_fingerprint(ev, login_hits[0])
                can_attempt = True
                if not run_once:
                    rec = action_gate["S1_LOGIN"]
                    can_attempt = bool(rec["epoch"] != state_epoch or rec["fp"] != fp or (float(now - float(rec["ts"])) >= MONITOR_ACTION_RETRY_SEC and not bool(rec["ok"])))
                if can_attempt:
                    acted, submit_happened = handle_login(ev, login_hits[0], username, password)
                else:
                    acted, submit_happened = False, False
                once_state_action_done["S1_LOGIN"] = True
                last_action_ts = time.time()
                if submit_happened:
                    last_login_submit_ts = last_action_ts
                action_performed = action_performed or acted
                action_gate["S1_LOGIN"] = {"epoch": state_epoch, "fp": fp, "ts": now, "ok": acted}
                last_action_name = "login"
                last_action_ok = acted
                last_window_fingerprint = fp
                ev.event("action_performed", state="S1_LOGIN", role="login", ok=acted)
                if acted:
                    emit_state_change(ev, state, "S2_DISCLAIMER", "login_completed")
                    state = "S2_DISCLAIMER"
                    state_epoch += 1
                    last_state_change_ts = time.time()
                    enter_state(ev, state)
                    failed_action_count = 0
                    no_window_backoff_sec = 1.0
                else:
                    failed_action_count += 1
            elif main_hits and not login_hits:
                emit_state_change(ev, state, "S2_DISCLAIMER", "login_window_not_present_main_visible")
                state = "S2_DISCLAIMER"
                state_epoch += 1
                last_state_change_ts = time.time()
                enter_state(ev, state)
                no_window_backoff_sec = 1.0
            else:
                ev.event("no_action", reason="login_window_not_found", state=state)
                if run_once:
                    ev.event("idle_exit", reason="login_window_not_found", state=state)
                    return 0
                time.sleep(no_window_backoff_sec)
                no_window_backoff_sec = min(NO_WINDOW_BACKOFF_MAX_SEC, no_window_backoff_sec * 2.0)
                continue

        if state == "S2_DISCLAIMER":
            if stage2_hits:
                top = stage2_hits[0]
                ev.event("stage2_detected", wid=top.get("id", ""), title=top.get("title", ""), wm_class=top.get("class", ""), score=int(top.get("score", "0")))
                emit_state_change(ev, state, "S4_DONE", "stage2_detected")
                state = "S4_DONE"
                state_epoch += 1
                last_state_change_ts = time.time()
                enter_state(ev, state)
                done_latched = True
                if run_once:
                    emit_done(ev, "stage2_detected")
                    return 0
                continue
            if disclaimer_hits and (now - last_action_ts) >= ACTION_COOLDOWN_SEC and (not run_once or not once_state_action_done["S2_DISCLAIMER"]):
                fp = window_fingerprint(ev, disclaimer_hits[0])
                can_attempt = True
                if not run_once:
                    rec = action_gate["S2_DISCLAIMER"]
                    can_attempt = bool(rec["epoch"] != state_epoch or rec["fp"] != fp or (float(now - float(rec["ts"])) >= MONITOR_ACTION_RETRY_SEC and not bool(rec["ok"])))
                acted = handle_disclaimer(ev, disclaimer_hits[0]) if can_attempt else False
                once_state_action_done["S2_DISCLAIMER"] = True
                last_action_ts = time.time()
                action_performed = action_performed or acted
                action_gate["S2_DISCLAIMER"] = {"epoch": state_epoch, "fp": fp, "ts": now, "ok": acted}
                last_action_name = "disclaimer"
                last_action_ok = acted
                last_window_fingerprint = fp
                ev.event("action_performed", state="S2_DISCLAIMER", role="disclaimer", ok=acted)
                if acted:
                    failed_action_count = 0
                    no_window_backoff_sec = 1.0
            elif main_hits and not disclaimer_hits:
                emit_state_change(ev, state, "S3_CLOSE_POPUPS", "no_disclaimer_main_visible")
                state = "S3_CLOSE_POPUPS"
                state_epoch += 1
                last_state_change_ts = time.time()
                enter_state(ev, state)
                done_candidate_since = 0.0
            else:
                ev.event("no_action", reason="disclaimer_window_not_found", state=state)
                if run_once:
                    ev.event("idle_exit", reason="disclaimer_window_not_found", state=state)
                    return 0
                time.sleep(no_window_backoff_sec)
                no_window_backoff_sec = min(NO_WINDOW_BACKOFF_MAX_SEC, no_window_backoff_sec * 2.0)
                continue

        if state == "S3_CLOSE_POPUPS":
            if stage2_hits:
                top = stage2_hits[0]
                ev.event("stage2_detected", wid=top.get("id", ""), title=top.get("title", ""), wm_class=top.get("class", ""), score=int(top.get("score", "0")))
                emit_state_change(ev, state, "S4_DONE", "stage2_detected")
                state = "S4_DONE"
                state_epoch += 1
                last_state_change_ts = time.time()
                enter_state(ev, state)
                done_latched = True
                if run_once:
                    emit_done(ev, "stage2_detected")
                    return 0
                continue
            if login_message_hits and (now - last_action_ts) >= ACTION_COOLDOWN_SEC and (not run_once or not once_state_action_done["S3_CLOSE_POPUPS"]):
                fp = window_fingerprint(ev, login_message_hits[0])
                can_attempt = True
                if not run_once:
                    rec = action_gate["S3_CLOSE_POPUPS"]
                    can_attempt = bool(rec["epoch"] != state_epoch or rec["fp"] != fp or (float(now - float(rec["ts"])) >= MONITOR_ACTION_RETRY_SEC and not bool(rec["ok"])))
                acted = handle_login_message_popup(ev, login_message_hits[0]) if can_attempt else False
                once_state_action_done["S3_CLOSE_POPUPS"] = True
                last_action_ts = time.time()
                action_performed = action_performed or acted
                action_gate["S3_CLOSE_POPUPS"] = {"epoch": state_epoch, "fp": fp, "ts": now, "ok": acted}
                last_action_name = "login_message_popup"
                last_action_ok = acted
                last_window_fingerprint = fp
                ev.event("action_performed", state="S3_CLOSE_POPUPS", role="login_message_popup", ok=acted)
                if acted:
                    failed_action_count = 0
                    no_window_backoff_sec = 1.0
            else:
                no_modals = not login_hits and not disclaimer_hits and not login_message_hits and not stage2_hits
                if main_hits and no_modals:
                    if done_candidate_since <= 0.0:
                        done_candidate_since = now
                    if (now - done_candidate_since) >= 10.0:
                        emit_state_change(ev, state, "S4_DONE", "main_visible_no_modals_10s")
                        state = "S4_DONE"
                        state_epoch += 1
                        last_state_change_ts = time.time()
                        enter_state(ev, state)
                        done_latched = True
                        if run_once:
                            emit_done(ev, "main_visible_no_modals_10s")
                            return 0
                        continue
                else:
                    done_candidate_since = 0.0
                ev.event("no_action", reason="no_ibkr_windows", state=state)
                if run_once:
                    ev.event("idle_exit", reason="no_window_found", state=state)
                    return 0
                time.sleep(no_window_backoff_sec)
                no_window_backoff_sec = min(NO_WINDOW_BACKOFF_MAX_SEC, no_window_backoff_sec * 2.0)
                continue

        if state == "S4_DONE":
            done_latched = True
            ev.event("idle_noop", reason="done_latched", windows_seen=len(windows))
            if run_once:
                emit_done(ev, "done_latched")
                return 0
            time.sleep(no_window_backoff_sec)
            no_window_backoff_sec = min(NO_WINDOW_BACKOFF_MAX_SEC, no_window_backoff_sec * 2.0)
            continue

        if state != loop_state_before:
            last_state_change_ts = time.time()
            last_stuck_key = ""

        if not run_once and len(pids) == 1 and not done_latched:
            elapsed = int(time.time() - last_state_change_ts)
            window_for_state: Optional[Dict[str, str]] = None
            reason = "no_fsm_progress"
            if state == "S1_LOGIN":
                window_for_state = login_hits[0] if login_hits else None
                reason = "login_window_not_found" if not window_for_state else ("login_attempted_but_window_persisted" if last_action_name == "login" else "login_window_found_but_focus_failed")
            elif state == "S2_DISCLAIMER":
                window_for_state = disclaimer_hits[0] if disclaimer_hits else None
                reason = "disclaimer_window_not_found" if not window_for_state else "disclaimer_click_attempted_but_persisted"
            elif state == "S3_CLOSE_POPUPS":
                window_for_state = login_message_hits[0] if login_message_hits else None
                reason = "login_messages_window_not_found" if not window_for_state else "login_messages_persisted_after_alt_f4"
            stuck_key = f"{state}|{reason}|{last_window_fingerprint}"
            if elapsed >= int(args.stuck_after_sec) and (stuck_key != last_stuck_key or (time.time() - last_stuck_emit_ts) >= int(args.stuck_cooldown_sec)):
                snapshot = {}
                if window_for_state:
                    snapshot = {
                        "wid": window_for_state.get("id", ""),
                        "wm_class": window_for_state.get("class", ""),
                        "title": window_for_state.get("title", ""),
                        "geometry": get_xwininfo_geometry(ev, window_for_state.get("id", "")),
                    }
                ev.event(
                    "stuck",
                    state=state,
                    reason=reason,
                    elapsed_sec=elapsed,
                    window_snapshot=snapshot,
                    last_action={"name": last_action_name, "ok": last_action_ok, "fingerprint": last_window_fingerprint},
                )
                last_stuck_emit_ts = time.time()
                last_stuck_key = stuck_key

        idle_counter += 1
        if not acted and idle_counter >= IDLE_EVENT_EVERY:
            ev.event("idle_noop", windows_seen=len(windows))
            idle_counter = 0

        if run_once:
            return 0 if action_performed else 1

        if run_once:
            loop_sleep = LOOP_SLEEP_SEC
        else:
            loop_sleep = MONITOR_POLL_SEC
        if not acted and failed_action_count > 0 and run_once:
            if failed_action_count <= ACTION_RETRY_MAX:
                time.sleep(1.0)
            else:
                time.sleep(NO_WINDOW_BACKOFF_MAX_SEC)
        else:
            time.sleep(loop_sleep)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        sys.exit(130)
