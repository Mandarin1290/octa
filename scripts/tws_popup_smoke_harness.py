#!/usr/bin/env python3
"""scripts/tws_popup_smoke_harness.py
TWS Popup Rule Smoke Harness.

Reads a JSON window snapshot and prints a deterministic step-by-step trace
showing which rule the engine would apply to each window and in what order.

Offline-safe: no X11 calls, no subprocess calls, no network.
Secrets-safe: window titles are printed but never credential values.

Usage
-----
# Use built-in example snapshot (simulates post-login TWS window state):
    python scripts/tws_popup_smoke_harness.py

# Use a real wmctrl snapshot captured via:
#   wmctrl -lp > /tmp/wmctrl_snapshot.txt
# Then convert to JSON (title is field 5+, pid is field 3):
    python scripts/tws_popup_smoke_harness.py --wmctrl-txt /tmp/wmctrl_snapshot.txt

# Use a pre-built JSON snapshot:
    python scripts/tws_popup_smoke_harness.py --snapshot /tmp/windows.json

# Write evidence artefacts to a directory:
    python scripts/tws_popup_smoke_harness.py --evidence-dir /tmp/smoke_evidence

Exit codes
----------
0  All matched popups would be closed (or no popups present).
1  One or more matched popups have no verify_absent action → would not confirm close.
2  Usage / argument error.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Make octa package importable when run from the repo root.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from octa.support.x11.popup_rules import (  # noqa: E402
    POPUP_REGISTRY,
    match_and_sort_windows,
    popup_rules_inventory,
)

# ---------------------------------------------------------------------------
# BUILT-IN EXAMPLE SNAPSHOT
# Simulates a realistic set of windows right after TWS completes login:
#   - Main TWS window (should NOT be matched)
#   - Login window (should NOT be matched — already dismissed by login chain)
#   - Known blocking popups that must be dismissed
# ---------------------------------------------------------------------------
EXAMPLE_SNAPSHOT: list[dict[str, str]] = [
    {
        "title": "Trader Workstation - PAPER U9999999",
        "wm_class": "ibcalpha.ibc.IbcTws",
        "pid": "12345",
        "wid": "0x01000001",
        "desktop": "0",
        "host": "hostname",
    },
    {
        "title": "Anmelden",
        "wm_class": "ibcalpha.ibc.IbcTws",
        "pid": "12345",
        "wid": "0x01000002",
        "desktop": "0",
        "host": "hostname",
    },
    {
        "title": "Warnhinweis",
        "wm_class": "ibcalpha.ibc.IbcTws",
        "pid": "12345",
        "wid": "0x01000003",
        "desktop": "0",
        "host": "hostname",
    },
    {
        "title": "Login Messages",
        "wm_class": "ibcalpha.ibc.IbcTws",
        "pid": "12345",
        "wid": "0x01000004",
        "desktop": "0",
        "host": "hostname",
    },
    {
        "title": "Dow Jones Heutige Top 10",
        "wm_class": "ibcalpha.ibc.IbcTws",
        "pid": "12345",
        "wid": "0x01000005",
        "desktop": "0",
        "host": "hostname",
    },
    {
        "title": "Börsenspiegel",
        "wm_class": "ibcalpha.ibc.IbcTws",
        "pid": "12345",
        "wid": "0x01000006",
        "desktop": "0",
        "host": "hostname",
    },
    {
        "title": "Programm wird geschlossen",
        "wm_class": "ibcalpha.ibc.IbcTws",
        "pid": "12345",
        "wid": "0x01000007",
        "desktop": "0",
        "host": "hostname",
    },
]


# ---------------------------------------------------------------------------
# wmctrl -lp text parser
# ---------------------------------------------------------------------------

def _parse_wmctrl_txt(raw: str) -> list[dict[str, str]]:
    """Parse the text output of ``wmctrl -lp`` into window dicts.

    Each line has the format::
        WID  DESKTOP  PID  HOST  TITLE...

    Host is replaced with empty string when unavailable.
    """
    windows: list[dict[str, str]] = []
    for line in raw.splitlines():
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
                "wm_class": "",  # wmctrl -lp does not provide WM_CLASS
            }
        )
    return windows


# ---------------------------------------------------------------------------
# Trace printing
# ---------------------------------------------------------------------------

def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _print(msg: str = "", *, file: Any = None) -> None:
    print(msg, file=file or sys.stdout)


def run_trace(
    windows: list[dict[str, str]],
    *,
    verbose: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    """Run the popup rule engine on a window list and return (trace, exit_code).

    Simulates the bounded drain loop (up to 60 iterations).
    Each iteration picks the highest-priority popup, records the action plan,
    then removes the window from the list (simulating successful dismissal).

    Returns
    -------
    trace : list[dict]
        JSON-serialisable record of every iteration.
    exit_code : int
        0 = all popups handled, 1 = at least one rule has no verify action.
    """
    MAX_ITERS = 60
    trace: list[dict[str, Any]] = []
    iteration = 0
    unverifiable_count = 0

    # ---- Initial classification: what's in the snapshot? ----
    all_matched = match_and_sort_windows(windows)
    non_matched = [w for w in windows if not any(w is mw for mw, _ in all_matched)]
    # Drain loop only processes matched (popup) windows.
    remaining = [mw for mw, _ in all_matched]

    _print(f"\n{'='*70}")
    _print("TWS POPUP RULE SMOKE HARNESS")
    _print(f"Timestamp : {_utc_iso()}")
    _print(f"Registry  : {len(POPUP_REGISTRY)} rules")
    _print(f"Windows   : {len(windows)} total")
    _print(f"  Matched : {len(all_matched)} (will be processed)")
    _print(f"  Skipped : {len(non_matched)} (no matching rule — not popup)")
    _print(f"{'='*70}\n")

    _print("=== SKIPPED WINDOWS (no popup rule matches) ===")
    for w in non_matched:
        _print(f"  SKIP  title={w.get('title', '')!r:50s}  wid={w.get('wid', '')}")
    if not non_matched:
        _print("  (none)")
    _print()

    _print("=== POPUP DRAIN LOOP (deterministic, max 60 iterations) ===")

    for iteration in range(1, MAX_ITERS + 1):
        if not remaining:
            _print(f"\nIter {iteration:2d}: no remaining windows — DONE")
            break

        matched = match_and_sort_windows(remaining)
        if not matched:
            _print(f"\nIter {iteration:2d}: no matching popups — DONE")
            break

        target_win, target_rule = matched[0]
        title = target_win.get("title", "")
        wid = target_win.get("wid", "?")
        pid = target_win.get("pid", "?")

        _print(f"\nIter {iteration:2d}: target title={title!r}")
        _print(f"        wid={wid}  pid={pid}")
        _print(f"        rule={target_rule.name!r}  priority={target_rule.priority}")
        if target_rule.suppress_checkbox_label:
            _print(f"        suppress_checkbox_label={target_rule.suppress_checkbox_label!r}")

        has_verify = False
        for action in target_rule.actions:
            tag = "[VERIFY]" if action.verify_absent else "       "
            chk = "[CHECKBOX]" if action.suppress_checkbox else "          "
            val = f" value={action.value!r}" if action.value else ""
            _print(f"        {tag} {chk} action.kind={action.kind!r}{val}")
            if action.verify_absent:
                has_verify = True

        if not has_verify:
            _print(f"        WARNING: rule {target_rule.name!r} has no verify_absent action")
            unverifiable_count += 1

        iter_record: dict[str, Any] = {
            "iteration": iteration,
            "target_title": title,
            "target_wid": wid,
            "target_pid": pid,
            "rule_name": target_rule.name,
            "rule_priority": target_rule.priority,
            "suppress_checkbox_label": target_rule.suppress_checkbox_label,
            "actions": [
                {
                    "kind": a.kind,
                    "value": a.value,
                    "verify_absent": a.verify_absent,
                    "suppress_checkbox": a.suppress_checkbox,
                }
                for a in target_rule.actions
            ],
            "result": "simulated_closed",
        }
        trace.append(iter_record)

        # Simulate dismissal: remove target from remaining list.
        remaining = [w for w in remaining if w is not target_win]

    if remaining:
        _print(f"\nWARNING: {len(remaining)} window(s) still remaining after {MAX_ITERS} iterations:")
        for w in remaining:
            _print(f"  REMAIN  title={w.get('title', '')!r}  wid={w.get('wid', '')}")

    _print(f"\n{'='*70}")
    _print(f"RESULT: {len(trace)} popup(s) processed in {iteration} iteration(s)")
    _print(f"        {len(remaining)} window(s) not processed")
    _print(f"        {unverifiable_count} rule(s) without verify_absent action")
    exit_code = 1 if unverifiable_count > 0 else 0
    _print(f"        Exit code: {exit_code}")
    _print(f"{'='*70}\n")

    return trace, exit_code


# ---------------------------------------------------------------------------
# Evidence writing
# ---------------------------------------------------------------------------

def _write_evidence(
    evidence_dir: Path,
    windows: list[dict[str, str]],
    trace: list[dict[str, Any]],
    exit_code: int,
) -> None:
    evidence_dir.mkdir(parents=True, exist_ok=True)
    stamp = _utc_iso()

    # popup_rules_inventory.json
    inv_path = evidence_dir / "popup_rules_inventory.json"
    inv_path.write_text(
        json.dumps(popup_rules_inventory(), indent=2) + "\n",
        encoding="utf-8",
    )

    # window_snapshot.json
    snap_path = evidence_dir / "window_snapshot.json"
    snap_path.write_text(
        json.dumps(windows, indent=2) + "\n",
        encoding="utf-8",
    )

    # deterministic_run_log.json
    run_path = evidence_dir / "deterministic_run_log.json"
    run_path.write_text(
        json.dumps(
            {
                "ts_utc": stamp,
                "source": "tws_popup_smoke_harness.py",
                "registry_rules": len(POPUP_REGISTRY),
                "input_windows": len(windows),
                "iterations": len(trace),
                "exit_code": exit_code,
                "trace": trace,
            },
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )

    _print(f"Evidence written to: {evidence_dir}")
    _print(f"  {inv_path.name}")
    _print(f"  {snap_path.name}")
    _print(f"  {run_path.name}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "TWS Popup Rule Smoke Harness — offline, deterministic window-rule trace.\n"
            "No X11, no subprocess, no network required."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    src = p.add_mutually_exclusive_group()
    src.add_argument(
        "--snapshot",
        metavar="FILE",
        help="JSON file: list of window dicts with 'title' (and optionally 'wm_class', 'wid', 'pid').",
    )
    src.add_argument(
        "--wmctrl-txt",
        metavar="FILE",
        help="Plain-text file: output of 'wmctrl -lp'.  Parsed automatically.",
    )
    p.add_argument(
        "--evidence-dir",
        metavar="DIR",
        help="Directory to write evidence artefacts (popup_rules_inventory.json, etc.).",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Extra output.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Load windows
    if args.snapshot:
        path = Path(args.snapshot)
        if not path.is_file():
            print(f"ERROR: --snapshot file not found: {path}", file=sys.stderr)
            return 2
        try:
            windows = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"ERROR: could not parse JSON snapshot: {exc}", file=sys.stderr)
            return 2
        if not isinstance(windows, list):
            print("ERROR: JSON snapshot must be a list of window dicts.", file=sys.stderr)
            return 2
    elif args.wmctrl_txt:
        path = Path(args.wmctrl_txt)
        if not path.is_file():
            print(f"ERROR: --wmctrl-txt file not found: {path}", file=sys.stderr)
            return 2
        windows = _parse_wmctrl_txt(path.read_text(encoding="utf-8"))
    else:
        _print("(no --snapshot or --wmctrl-txt given — using built-in example snapshot)\n")
        windows = EXAMPLE_SNAPSHOT

    trace, exit_code = run_trace(windows, verbose=args.verbose)

    if args.evidence_dir:
        _write_evidence(Path(args.evidence_dir), windows, trace, exit_code)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
