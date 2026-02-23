"""octa/support/x11/x11_actions.py
X11 window management actions for TWS popup dismissal.

Caller-injectable command runner pattern: all shell calls go through
``run_cmd: RunCmd`` so callers can mock them without X11/subprocess.

Offline-safe: this module itself does no I/O.  Callers supply ``run_cmd``.
Secrets-safe: only window IDs and titles are logged, never credentials.

Typical production use::

    import subprocess

    def run_cmd(cmd: list[str]) -> tuple[int, str]:
        cp = subprocess.run(cmd, capture_output=True, text=True, check=False,
                            timeout=5.0, env=root_env)
        return cp.returncode, cp.stdout or ""

    from octa.support.x11.x11_actions import close_window_ladder
    closed = close_window_ladder("0x01e00003", run_cmd)
"""
from __future__ import annotations

import time
from typing import Callable

# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------

# Caller-provided runner: takes a list of command args, returns (returncode, stdout).
RunCmd = Callable[[list[str]], tuple[int, str]]


# ---------------------------------------------------------------------------
# Window enumeration
# ---------------------------------------------------------------------------


def list_windows(run_cmd: RunCmd) -> list[dict[str, str]]:
    """Return a stable sorted list of open windows via ``wmctrl -lp``.

    Each dict contains: ``wid``, ``desktop``, ``pid``, ``host``, ``title``.
    Sorted deterministically ascending by ``wid``.

    Returns an empty list if wmctrl fails or produces no output.
    """
    rc, out = run_cmd(["wmctrl", "-lp"])
    if rc != 0:
        return []
    windows: list[dict[str, str]] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 4)
        if len(parts) < 5:
            continue
        windows.append(
            {
                "wid": parts[0],
                "desktop": parts[1],
                "pid": parts[2],
                "host": parts[3],
                "title": parts[4],
            }
        )
    windows.sort(key=lambda w: w["wid"])
    return windows


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _wid_present(wid: str, run_cmd: RunCmd) -> bool:
    """Return True if ``wid`` still appears in the current wmctrl window list."""
    wins = list_windows(run_cmd)
    wid_l = wid.strip().lower()
    return any(w["wid"].strip().lower() == wid_l for w in wins)


# ---------------------------------------------------------------------------
# Close ladder
# ---------------------------------------------------------------------------


def close_window_ladder(
    wid: str,
    run_cmd: RunCmd,
    *,
    step_sleep_sec: float = 0.2,
) -> bool:
    """Deterministic close sequence for a single popup window.

    Executes steps in strict order; returns ``True`` on the first step at which
    ``wid`` is absent from the wmctrl window list (i.e. window was closed).
    Returns ``False`` if the window survives all steps.

    Steps
    -----
    1. ``wmctrl -ia <wid>``    — activate / bring to focus (no verify)
    2. ``xdotool key Escape``  — attempt Escape dismiss        [verify]
    3. ``xdotool key Return``  — attempt Return accept         [verify]
    4. ``xdotool key KP_Enter``— numpad Enter variant          [verify]
    5. ``xdotool key alt+F4``  — force close via Alt+F4        [verify]
    6. ``wmctrl -ic <wid>``    — WM_DELETE_WINDOW as fallback  [verify]

    Parameters
    ----------
    wid:
        Window ID hex string (e.g. ``"0x01e00001"``).
    run_cmd:
        Caller-provided runner: ``cmd_args -> (returncode, stdout)``.
        For production: wrap ``subprocess.run``.  For tests: supply a mock.
    step_sleep_sec:
        Seconds to pause after each action before re-enumerating.
        Set to ``0.0`` in unit tests for speed; ``0.2`` in production.
    """
    # Step 1: activate (focus).  Not a dismiss action; no verify.
    run_cmd(["wmctrl", "-ia", wid])
    if step_sleep_sec > 0:
        time.sleep(step_sleep_sec)

    dismiss_steps: list[list[str]] = [
        ["xdotool", "key", "Escape"],    # step 2
        ["xdotool", "key", "Return"],    # step 3
        ["xdotool", "key", "KP_Enter"],  # step 4
        ["xdotool", "key", "alt+F4"],    # step 5
        ["wmctrl", "-ic", wid],          # step 6
    ]

    for cmd in dismiss_steps:
        run_cmd(cmd)
        if step_sleep_sec > 0:
            time.sleep(step_sleep_sec)
        if not _wid_present(wid, run_cmd):
            return True  # window confirmed gone

    return False  # window survived all steps
