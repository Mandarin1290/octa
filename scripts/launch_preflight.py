#!/usr/bin/env python3
"""System health go/no-go preflight for paper/shadow launch.

Checks all circuit breakers, data freshness, model artifacts, and risk state
before activating trading.  Exit code 0 = GO, 1 = NO-GO.

Usage:
    python scripts/launch_preflight.py [--mode paper|dry-run]
    python scripts/launch_preflight.py --state-dir /path/to/state
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_PASS = "✅ PASS"
_WARN = "⚠️  WARN"
_FAIL = "❌ FAIL"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _check(label: str, ok: bool, detail: str = "", warn_only: bool = False) -> bool:
    """Print one check line; return True if hard-fail (ok=False and not warn_only)."""
    tag = _PASS if ok else (_WARN if warn_only else _FAIL)
    suffix = f" — {detail}" if detail else ""
    print(f"  {tag}  {label}{suffix}")
    return not ok and not warn_only


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def check_altdata_freshness() -> tuple[bool, str]:
    altdat = Path("octa") / "var" / "altdata" / "altdat.duckdb"
    if not altdat.exists():
        return False, f"altdat.duckdb not found at {altdat}"
    age_h = (_now().timestamp() - os.path.getmtime(altdat)) / 3600
    threshold = 48.0
    if age_h > threshold:
        return False, f"stale by {age_h:.1f}h (limit {threshold:.0f}h)"
    return True, f"age {age_h:.1f}h (fresh)"


def check_paper_ready_models(paper_ready_dir: Path) -> tuple[bool, str]:
    if not paper_ready_dir.exists():
        return False, f"{paper_ready_dir} does not exist"
    pkls = list(paper_ready_dir.rglob("*.pkl"))
    if not pkls:
        return False, f"no .pkl artifacts under {paper_ready_dir}"
    return True, f"{len(pkls)} artifact(s)"


def check_drawdown(state_dir: Path) -> tuple[bool, str]:
    hwm_file = state_dir / "nav_hwm.json"
    cap_file = state_dir / "capital_state.json"
    if not hwm_file.exists():
        return True, "no HWM yet (first run)"
    try:
        hwm = float(json.loads(hwm_file.read_text())["hwm_nav"])
    except Exception as e:
        return False, f"nav_hwm.json unreadable: {e}"
    if not cap_file.exists() or hwm <= 0:
        return True, f"HWM={hwm:.0f}"
    try:
        nav = float(json.loads(cap_file.read_text())["nav"])
        dd = max(0.0, 1.0 - nav / hwm)
        limit = 0.15
        if dd > limit:
            return False, f"drawdown {dd*100:.1f}% > {limit*100:.0f}% limit (HWM={hwm:.0f}, NAV={nav:.0f})"
        return True, f"drawdown {dd*100:.1f}% (HWM={hwm:.0f}, NAV={nav:.0f})"
    except Exception as e:
        return True, f"HWM={hwm:.0f} (cannot read capital_state: {e})"


def check_daily_loss(state_dir: Path) -> tuple[bool, str]:
    """Check kill-switch daily loss via nav_day_open.json vs capital_state.json."""
    open_file = state_dir / "nav_day_open.json"
    cap_file = state_dir / "capital_state.json"
    today = _now().strftime("%Y-%m-%d")
    if not open_file.exists() or not cap_file.exists():
        return True, "no day-open data (first run)"
    try:
        d = json.loads(open_file.read_text())
        if d.get("date") != today:
            return True, "day-open is from prior day — will reset on next run"
        nav_open = float(d["nav"])
        nav_now = float(json.loads(cap_file.read_text())["nav"])
        if nav_open <= 0:
            return True, "nav_open=0"
        loss = max(0.0, (nav_open - nav_now) / nav_open)
        limit = 0.05
        if loss >= limit:
            return False, f"daily loss {loss*100:.1f}% >= {limit*100:.0f}% kill-switch threshold"
        return True, f"daily loss {loss*100:.1f}% (open={nav_open:.0f}, last={nav_now:.0f})"
    except Exception as e:
        return True, f"cannot compute daily loss: {e}"


def check_loss_streak(state_dir: Path) -> tuple[bool, str]:
    streak_file = state_dir / "loss_streak.json"
    if not streak_file.exists():
        return True, "no prior streak"
    try:
        d = json.loads(streak_file.read_text())
        streak = int(d.get("streak", 0))
        pct = float(d.get("daily_loss_pct", 0.0))
    except Exception:
        return True, "unreadable streak file"
    if streak >= 5:
        return False, f"streak={streak} days (last daily_loss={pct:.2f}%)"
    if streak > 0:
        return True, f"streak={streak} days (below 5-day limit)"
    return True, "streak=0"


def check_drift_registry(drift_dir: Path) -> tuple[bool, str]:
    if not drift_dir.exists():
        return True, "registry absent"
    breaches = [
        f.stem for f in sorted(drift_dir.glob("*.json"))
        if not json.loads(f.read_text()).get("disabled")
    ]
    if breaches:
        return False, f"{len(breaches)} breach(es): {', '.join(breaches[:4])}"
    return True, "no active breaches"


def check_fills_today(state_dir: Path) -> tuple[bool, str]:
    """Informational: count today's orders."""
    try:
        from octa.execution.fill_tracker import FillTracker
        today = _now().strftime("%Y-%m-%d")
        s = FillTracker(state_dir).summary_for_date(today)
        if s["total"] == 0:
            return True, f"no fills today ({today})"
        return True, f"{s['total']} orders ({s['filled']} filled) today"
    except Exception as e:
        return True, f"fill tracker unavailable: {e}"


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run(mode: str, state_dir: Path, paper_ready_dir: Path, drift_dir: Path) -> bool:
    ts = _now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*64}")
    print(f"  OCTA Launch Preflight   mode={mode}   {ts} UTC")
    print(f"{'='*64}\n")

    hard_fails = 0

    print("── Data ─────────────────────────────────────────────────────")
    ok, det = check_altdata_freshness()
    hard_fails += _check("AltData (FRED) cache", ok, det)

    print("\n── Models ───────────────────────────────────────────────────")
    ok, det = check_paper_ready_models(paper_ready_dir)
    hard_fails += _check("Paper-ready artifacts", ok, det)

    print("\n── Risk circuit breakers ────────────────────────────────────")
    ok, det = check_drawdown(state_dir)
    hard_fails += _check("Portfolio drawdown (HWM)", ok, det)

    ok, det = check_daily_loss(state_dir)
    hard_fails += _check("Daily NAV loss (kill switch)", ok, det)

    ok, det = check_loss_streak(state_dir)
    hard_fails += _check("Consecutive loss streak", ok, det)

    ok, det = check_drift_registry(drift_dir)
    hard_fails += _check("Model drift registry", ok, det)

    print("\n── Execution log ────────────────────────────────────────────")
    ok, det = check_fills_today(state_dir)
    _check("Today's fill log", ok, det, warn_only=True)

    print(f"\n{'='*64}")
    if hard_fails == 0:
        print(f"  🟢  GO — {mode.upper()} launch is clear")
    else:
        print(f"  🔴  NO-GO — {hard_fails} blocker(s) must be resolved before launch")
    print(f"{'='*64}\n")
    return hard_fails == 0


def main() -> None:
    p = argparse.ArgumentParser(description="OCTA launch preflight — system health go/no-go")
    p.add_argument("--mode", default="paper", choices=["paper", "dry-run", "shadow", "live"])
    p.add_argument("--state-dir", default="octa/var/state")
    p.add_argument("--paper-ready-dir", default="octa/var/models/paper_ready")
    p.add_argument("--drift-dir", default="octa/var/registry/models/drift")
    args = p.parse_args()
    go = run(
        mode=args.mode,
        state_dir=Path(args.state_dir),
        paper_ready_dir=Path(args.paper_ready_dir),
        drift_dir=Path(args.drift_dir),
    )
    sys.exit(0 if go else 1)


if __name__ == "__main__":
    main()
