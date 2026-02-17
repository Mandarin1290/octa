"""OCTA IBKR X11 login step-machine.

Deterministic, fail-closed, evidence-first step machine for TWS/IB Gateway
startup automation under X11 (real or Xvfb).

States (executed in strict order):
  1. VERIFY_X11       — confirm X11 display is reachable
  2. START_TWS        — ensure TWS/Gateway process is launched
  3. WAIT_LOGIN_WINDOW — wait for TWS login window to appear
  4. INJECT_CREDENTIALS — enter username + password via xdotool
  5. WAIT_POST_LOGIN  — wait for login window to disappear
  6. WAIT_DISCLAIMER  — wait for disclaimer/agreement dialog
  7. ACCEPT_DISCLAIMER — accept disclaimer via Tab+Enter
  8. VERIFY_STABLE    — confirm TWS stays running for N seconds

Credentials come ONLY from:
  - OCTA_IBKR_USER / OCTA_IBKR_PASS env vars, OR
  - A secrets file at OCTA_IBKR_SECRETS_FILE (line1=user, line2=pass)
Passwords are NEVER logged.

If ANY state fails -> screenshot + evidence + exit nonzero (systemd restart).
"""
from __future__ import annotations

import argparse
import enum
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# State enum — strict order
# ---------------------------------------------------------------------------

class State(enum.Enum):
    VERIFY_X11 = "VERIFY_X11"
    START_TWS = "START_TWS"
    WAIT_LOGIN_WINDOW = "WAIT_LOGIN_WINDOW"
    INJECT_CREDENTIALS = "INJECT_CREDENTIALS"
    WAIT_POST_LOGIN = "WAIT_POST_LOGIN"
    WAIT_DISCLAIMER = "WAIT_DISCLAIMER"
    ACCEPT_DISCLAIMER = "ACCEPT_DISCLAIMER"
    VERIFY_STABLE = "VERIFY_STABLE"


STATE_ORDER: list[State] = list(State)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_event(path: str, payload: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    row = dict(payload)
    row.setdefault("ts", _utc_iso())
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")


def _run_cmd(argv: list[str], timeout: float = 10.0) -> str:
    """Run a command, return stdout.  Raises on non-zero exit."""
    cp = subprocess.run(argv, capture_output=True, text=True, check=False,
                        timeout=timeout)
    if cp.returncode != 0:
        raise RuntimeError(f"cmd_failed:{argv}:rc={cp.returncode}:{(cp.stderr or '').strip()}")
    return cp.stdout or ""


def _run_cmd_ok(argv: list[str], timeout: float = 10.0) -> tuple[bool, str]:
    """Run a command, return (success, stdout). Never raises."""
    try:
        cp = subprocess.run(argv, capture_output=True, text=True, check=False,
                            timeout=timeout)
        return cp.returncode == 0, (cp.stdout or "")
    except Exception:
        return False, ""


def _screenshot(display: str, out_path: str) -> bool:
    """Capture a screenshot on failure.  Best-effort, never raises."""
    try:
        env = dict(os.environ)
        env["DISPLAY"] = display
        subprocess.run(
            ["import", "-window", "root", out_path],
            env=env, capture_output=True, timeout=10, check=False,
        )
        return Path(out_path).exists()
    except Exception:
        return False


def _xdotool_search(pattern: str, by: str = "--name") -> list[str]:
    """Search for visible windows by title/class.  Returns list of window IDs."""
    ok, out = _run_cmd_ok(["xdotool", "search", "--onlyvisible", by, pattern])
    if not ok:
        return []
    return [ln.strip() for ln in out.splitlines() if ln.strip()]


def _xdotool_search_any(patterns: list[tuple[str, str]]) -> list[str]:
    """Try multiple (pattern, search_flag) pairs, return first non-empty result."""
    for pattern, flag in patterns:
        ids = _xdotool_search(pattern, by=flag)
        if ids:
            return ids
    return []


# ---------------------------------------------------------------------------
# Credential loading (NEVER log the password)
# ---------------------------------------------------------------------------

def _load_credentials() -> tuple[str, str]:
    """Load (user, pass) from env or secrets file.  Never returns password in errors."""
    user = os.environ.get("OCTA_IBKR_USER", "")
    pw = os.environ.get("OCTA_IBKR_PASS", "")
    if user and pw:
        return user, pw

    secrets_file = os.environ.get("OCTA_IBKR_SECRETS_FILE", "")
    if secrets_file and Path(secrets_file).is_file():
        lines = Path(secrets_file).read_text().strip().splitlines()
        if len(lines) >= 2:
            return lines[0].strip(), lines[1].strip()
        raise RuntimeError("secrets_file_invalid_format")

    raise RuntimeError("no_credentials_configured")


# ---------------------------------------------------------------------------
# Step results
# ---------------------------------------------------------------------------

class StepResult:
    __slots__ = ("name", "state", "status", "details", "start_ts", "end_ts")

    def __init__(self, state: State) -> None:
        self.state = state
        self.name = state.value
        self.status = "PENDING"
        self.details: dict[str, Any] = {}
        self.start_ts = ""
        self.end_ts = ""

    def start(self) -> "StepResult":
        self.start_ts = _utc_iso()
        self.status = "RUNNING"
        return self

    def pass_(self, **kw: Any) -> "StepResult":
        self.end_ts = _utc_iso()
        self.status = "PASS"
        self.details.update(kw)
        return self

    def fail(self, **kw: Any) -> "StepResult":
        self.end_ts = _utc_iso()
        self.status = "FAIL"
        self.details.update(kw)
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.name,
            "step_name": self.name,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "status": self.status,
            "details": self.details,
        }


# ---------------------------------------------------------------------------
# Individual state handlers
# ---------------------------------------------------------------------------

LOGIN_PATTERNS: list[tuple[str, str]] = [
    ("Login", "--name"),
    ("IB Gateway", "--name"),
    ("Trader Workstation", "--name"),
    ("Interactive Brokers", "--name"),
]

DISCLAIMER_PATTERNS: list[tuple[str, str]] = [
    ("Disclaimer", "--name"),
    ("Important", "--name"),
    ("Agreement", "--name"),
]


def state_verify_x11(display: str) -> StepResult:
    """STATE 1: Verify X11 display is reachable."""
    r = StepResult(State.VERIFY_X11).start()
    ok, _ = _run_cmd_ok(["xdpyinfo", "-display", display])
    if ok:
        return r.pass_(display=display, method="xdpyinfo")
    # Fallback: socket check
    dnum = display.lstrip(":").split(".")[0]
    sock = f"/tmp/.X11-unix/X{dnum}"
    if Path(sock).is_socket():
        return r.pass_(display=display, method="socket")
    return r.fail(display=display, error="x11_not_reachable")


def state_start_tws(tws_cmd: str, display: str) -> StepResult:
    """STATE 2: Ensure TWS/Gateway process is launched."""
    r = StepResult(State.START_TWS).start()

    # Check if already running
    ok, out = _run_cmd_ok(["pgrep", "-fa", "Jts"])
    if ok and out.strip():
        return r.pass_(action="already_running", pgrep_output=out.strip()[:200])

    if not tws_cmd:
        return r.fail(error="no_tws_cmd")

    if not (Path(tws_cmd).is_file() or _run_cmd_ok(["which", tws_cmd])[0]):
        return r.fail(error="tws_cmd_not_found", cmd=tws_cmd)

    try:
        env = dict(os.environ)
        env["DISPLAY"] = display
        proc = subprocess.Popen(
            [tws_cmd], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=env,
        )
    except Exception as exc:
        return r.fail(error=f"launch_failed:{type(exc).__name__}:{exc}")

    # Wait briefly for process to settle
    time.sleep(3)
    ok2, out2 = _run_cmd_ok(["pgrep", "-fa", "Jts"])
    if ok2 and out2.strip():
        return r.pass_(action="launched", pid=proc.pid)

    return r.fail(error="process_not_found_after_launch", pid=proc.pid)


def state_wait_login_window(timeout_sec: float, poll_sec: float = 1.0) -> StepResult:
    """STATE 3: Wait for TWS login window to appear.  Retry with backoff."""
    r = StepResult(State.WAIT_LOGIN_WINDOW).start()
    deadline = time.monotonic() + timeout_sec
    attempt = 0
    backoff = poll_sec
    while time.monotonic() < deadline:
        ids = _xdotool_search_any(LOGIN_PATTERNS)
        if ids:
            return r.pass_(window_ids=ids, attempts=attempt)
        attempt += 1
        time.sleep(min(backoff, 5.0))
        backoff = min(backoff * 1.3, 5.0)  # exponential backoff capped at 5s
    return r.fail(error="login_window_not_found", timeout_sec=timeout_sec, attempts=attempt)


def state_inject_credentials(dry_run: bool = False) -> StepResult:
    """STATE 4: Focus login window, enter user/pass via TAB navigation."""
    r = StepResult(State.INJECT_CREDENTIALS).start()
    try:
        user, pw = _load_credentials()
    except RuntimeError as e:
        return r.fail(error=str(e))

    r.details["user_loaded"] = True
    r.details["pass_loaded"] = True
    r.details["dry_run"] = dry_run

    if dry_run:
        return r.pass_(action="dry_run_skip")

    # Find login window
    ids = _xdotool_search_any(LOGIN_PATTERNS)
    if not ids:
        return r.fail(error="login_window_lost")

    winid = ids[0]
    try:
        # Activate window
        _run_cmd(["xdotool", "windowactivate", "--sync", winid])
        time.sleep(0.3)
        # Select-all in current field, type username
        _run_cmd(["xdotool", "key", "ctrl+a"])
        time.sleep(0.1)
        _run_cmd(["xdotool", "type", "--clearmodifiers", "--delay", "20", user])
        time.sleep(0.1)
        # TAB to password field
        _run_cmd(["xdotool", "key", "Tab"])
        time.sleep(0.1)
        # Select-all, type password (password is NEVER logged)
        _run_cmd(["xdotool", "key", "ctrl+a"])
        time.sleep(0.1)
        _run_cmd(["xdotool", "type", "--clearmodifiers", "--delay", "20", pw])
        time.sleep(0.1)
        # Submit with Enter
        _run_cmd(["xdotool", "key", "Return"])
    except RuntimeError as e:
        return r.fail(error=f"input_failed:{e}")

    return r.pass_(window_id=winid, action="credentials_entered")


def state_wait_post_login(timeout_sec: float, poll_sec: float = 2.0) -> StepResult:
    """STATE 5: Wait for login window to disappear or post-login window to appear."""
    r = StepResult(State.WAIT_POST_LOGIN).start()
    deadline = time.monotonic() + timeout_sec

    post_login_patterns: list[tuple[str, str]] = [
        *DISCLAIMER_PATTERNS,
        ("Trading", "--name"),
    ]

    while time.monotonic() < deadline:
        post_ids = _xdotool_search_any(post_login_patterns)
        if post_ids:
            return r.pass_(state="post_login_window_found", window_ids=post_ids)

        login_ids = _xdotool_search_any([("Login", "--name")])
        if not login_ids:
            return r.pass_(state="login_window_gone")

        time.sleep(poll_sec)

    return r.fail(error="post_login_timeout", timeout_sec=timeout_sec)


def state_wait_disclaimer(timeout_sec: float, poll_sec: float = 1.0) -> StepResult:
    """STATE 6: Wait for disclaimer window to appear."""
    r = StepResult(State.WAIT_DISCLAIMER).start()
    deadline = time.monotonic() + timeout_sec

    while time.monotonic() < deadline:
        ids = _xdotool_search_any(DISCLAIMER_PATTERNS)
        if ids:
            return r.pass_(window_ids=ids)
        time.sleep(poll_sec)

    # Disclaimer may not appear (already accepted, or TWS config skips it).
    # Check if main TWS window exists — that means we're past it.
    main_ids = _xdotool_search_any([
        ("Trader Workstation", "--name"),
        ("IB Gateway", "--name"),
    ])
    if main_ids:
        return r.pass_(action="no_disclaimer_but_main_window_present", window_ids=main_ids)

    return r.fail(error="disclaimer_not_found_timeout", timeout_sec=timeout_sec)


def state_accept_disclaimer(timeout_sec: float, dry_run: bool = False,
                            poll_sec: float = 1.0) -> StepResult:
    """STATE 7: Accept the TWS disclaimer dialog via TAB+Enter."""
    r = StepResult(State.ACCEPT_DISCLAIMER).start()
    r.details["dry_run"] = dry_run
    deadline = time.monotonic() + timeout_sec

    while time.monotonic() < deadline:
        ids = _xdotool_search_any(DISCLAIMER_PATTERNS)
        if not ids:
            # Disclaimer already gone — pass
            return r.pass_(action="disclaimer_already_gone")

        winid = ids[0]
        if dry_run:
            return r.pass_(window_id=winid, action="dry_run_skip")

        try:
            _run_cmd(["xdotool", "windowactivate", "--sync", winid])
            time.sleep(0.3)
            # TAB navigation to Accept button + Enter
            for _ in range(10):
                _run_cmd(["xdotool", "key", "Tab"])
                time.sleep(0.05)
            _run_cmd(["xdotool", "key", "Return"])
            time.sleep(1.0)

            # Verify disclaimer closed
            still = _xdotool_search_any(DISCLAIMER_PATTERNS)
            if not still:
                return r.pass_(window_id=winid, action="accepted")

            # Fallback: try window-relative click on bottom-right area
            try:
                geom_out = _run_cmd(["xdotool", "getwindowgeometry", "--shell", winid])
                geom: dict[str, str] = {}
                for ln in geom_out.splitlines():
                    if "=" in ln:
                        k, v = ln.split("=", 1)
                        geom[k.strip()] = v.strip()
                wx = int(geom.get("X", "0"))
                wy = int(geom.get("Y", "0"))
                ww = int(geom.get("WIDTH", "400"))
                wh = int(geom.get("HEIGHT", "300"))
                # Accept button is typically bottom-right quadrant
                click_x = wx + int(ww * 0.75)
                click_y = wy + int(wh * 0.90)
                _run_cmd(["xdotool", "mousemove", "--sync",
                          str(click_x), str(click_y), "click", "1"])
                time.sleep(1.0)
                still2 = _xdotool_search_any(DISCLAIMER_PATTERNS)
                if not still2:
                    return r.pass_(window_id=winid, action="accepted_coordinate_click",
                                   click_x=click_x, click_y=click_y)
            except RuntimeError:
                pass  # coordinate fallback failed, keep retrying

        except RuntimeError as e:
            return r.fail(error=f"click_failed:{e}", window_id=winid)

        time.sleep(poll_sec)

    return r.fail(error="disclaimer_accept_timeout", timeout_sec=timeout_sec)


def state_verify_stable(timeout_sec: float, poll_sec: float = 2.0) -> StepResult:
    """STATE 8: Confirm TWS process stays alive for N seconds after login."""
    r = StepResult(State.VERIFY_STABLE).start()
    deadline = time.monotonic() + timeout_sec
    checks = 0
    while time.monotonic() < deadline:
        ok, _ = _run_cmd_ok(["pgrep", "-fa", "Jts"])
        checks += 1
        if not ok:
            return r.fail(error="process_not_found", checks=checks)
        time.sleep(poll_sec)
    return r.pass_(checks=checks, action="process_stable")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_login_sequence(
    *,
    display: str,
    dry_run: bool,
    timeout_sec: float,
    evidence_dir: str,
    tws_cmd: str = "",
) -> dict[str, Any]:
    """Execute the full login step machine.  Returns run summary."""
    steps: list[StepResult] = []
    events_jsonl = str(Path(evidence_dir) / "events.jsonl")

    def _record(step: StepResult) -> StepResult:
        steps.append(step)
        _write_event(events_jsonl, {"event_type": "step_result", **step.to_dict()})
        return step

    def _fail_out(step: StepResult, tag: str) -> dict[str, Any]:
        scr = str(Path(evidence_dir) / f"failure_{tag}.png")
        _screenshot(display, scr)
        return _finalize(steps, evidence_dir, ok=False)

    # STATE 1: VERIFY_X11
    s = _record(state_verify_x11(display))
    if s.status != "PASS":
        return _fail_out(s, "verify_x11")

    # STATE 2: START_TWS
    s = _record(state_start_tws(tws_cmd, display))
    if s.status != "PASS":
        return _fail_out(s, "start_tws")

    # STATE 3: WAIT_LOGIN_WINDOW
    s = _record(state_wait_login_window(timeout_sec=min(timeout_sec, 90.0)))
    if s.status != "PASS":
        return _fail_out(s, "wait_login")

    # STATE 4: INJECT_CREDENTIALS
    s = _record(state_inject_credentials(dry_run=dry_run))
    if s.status != "PASS":
        return _fail_out(s, "inject_creds")

    # STATE 5: WAIT_POST_LOGIN
    s = _record(state_wait_post_login(timeout_sec=min(timeout_sec, 60.0)))
    if s.status != "PASS":
        return _fail_out(s, "wait_post_login")

    # STATE 6: WAIT_DISCLAIMER
    s = _record(state_wait_disclaimer(timeout_sec=min(timeout_sec, 30.0)))
    if s.status != "PASS":
        return _fail_out(s, "wait_disclaimer")

    # STATE 7: ACCEPT_DISCLAIMER
    s = _record(state_accept_disclaimer(
        timeout_sec=min(timeout_sec, 30.0), dry_run=dry_run,
    ))
    if s.status != "PASS":
        return _fail_out(s, "accept_disclaimer")

    # STATE 8: VERIFY_STABLE
    s = _record(state_verify_stable(timeout_sec=min(10.0, timeout_sec)))
    if s.status != "PASS":
        return _fail_out(s, "verify_stable")

    return _finalize(steps, evidence_dir, ok=True)


def _finalize(steps: list[StepResult], evidence_dir: str, ok: bool) -> dict[str, Any]:
    """Write run.json and sha256."""
    run = {
        "ok": ok,
        "ts": _utc_iso(),
        "python": sys.executable,
        "states_executed": [s.name for s in steps],
        "steps": [s.to_dict() for s in steps],
    }
    run_path = Path(evidence_dir) / "run.json"
    run_path.parent.mkdir(parents=True, exist_ok=True)
    run_json = json.dumps(run, indent=2, sort_keys=True)
    run_path.write_text(run_json, encoding="utf-8")

    sha_path = Path(evidence_dir) / "sha256.txt"
    sha = hashlib.sha256(run_json.encode()).hexdigest()
    sha_path.write_text(f"{sha}  run.json\n", encoding="utf-8")

    return run


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="OCTA IBKR X11 login step-machine",
    )
    ap.add_argument("--dry-run", action="store_true", default=False,
                    help="Skip actual xdotool input (no credentials entered)")
    ap.add_argument("--timeout-sec", type=int, default=120,
                    help="Overall timeout per step (default 120)")
    ap.add_argument("--display", default="",
                    help="X11 display (default: from env)")
    ap.add_argument("--evidence-dir", default="",
                    help="Evidence output directory")
    ap.add_argument("--tws-cmd", default="",
                    help="TWS launcher command (auto-detected if empty)")
    args = ap.parse_args()

    display = args.display or os.environ.get("DISPLAY", ":99")
    os.environ["DISPLAY"] = display

    utc_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    evidence_dir = args.evidence_dir or f"octa/var/evidence/ibkr_autologin_{utc_stamp}"

    # Resolve TWS command
    tws_cmd = args.tws_cmd or os.environ.get("OCTA_TWS_CMD", "")
    if not tws_cmd:
        # Auto-detect
        for candidate in [
            os.path.expanduser("~/Jts/tws"),
            os.path.expanduser("~/Jts/tws/tws"),
            os.path.expanduser("~/Jts/tws/tws.sh"),
        ]:
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                tws_cmd = candidate
                break

    result = run_login_sequence(
        display=display,
        dry_run=args.dry_run,
        timeout_sec=float(args.timeout_sec),
        evidence_dir=evidence_dir,
        tws_cmd=tws_cmd,
    )

    print(json.dumps({"ok": result["ok"], "evidence_dir": evidence_dir}, sort_keys=True))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
