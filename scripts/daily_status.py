#!/usr/bin/env python3
"""OCTA Daily Status Report — one-screen system health check.

Usage:
    python scripts/daily_status.py               # terminal output
    python scripts/daily_status.py --telegram    # + send to Telegram
    python scripts/daily_status.py --compact     # one-liner per section
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

_REGISTRY = Path("artifacts/registry.sqlite3")
_EVIDENCE_BASE = Path("octa/var/evidence")
_LOGS_DIR = Path("octa/var/logs")
_QUEUE_FILE = Path("octa/var/screening_queue.json")
_SHADOW_ORDERS = Path("artifacts/shadow_orders.ndjson")


# ─────────────────────────── data collectors ────────────────────────────────

def _processes() -> dict:
    checks = {
        "shadow_loop": "shadow_trading_loop",
        "screening": "run_universe_screening",
        "paper_runner": "run_paper_live",
    }
    out = {}
    for name, pattern in checks.items():
        r = subprocess.run(["pgrep", "-f", pattern], capture_output=True)
        out[name] = r.returncode == 0
    return out


def _registry_state() -> list[dict]:
    if not _REGISTRY.exists():
        return []
    try:
        db = sqlite3.connect(_REGISTRY)
        rows = db.execute(
            "SELECT symbol, timeframe, lifecycle_status FROM artifacts ORDER BY symbol, timeframe"
        ).fetchall()
        db.close()
        return [{"symbol": r[0], "timeframe": r[1], "status": r[2]} for r in rows]
    except Exception:
        return []


def _candidates() -> tuple[list[str], list[str]]:
    """Return (full_pass, near_miss) symbol lists from evidence."""
    full_pass, near_miss = [], []
    for result_json in glob.glob(str(_EVIDENCE_BASE / "universe_screen*/results/*.json")):
        sym = Path(result_json).stem
        try:
            d = json.load(open(result_json))
            if not d.get("paper_ready"):
                near_miss.append(sym) if (d.get("artifact_summary", {}).get("valid_tradeable_artifacts", 0) > 0) else None
                continue
            stages = d.get("stages", [])
            h1 = any(isinstance(s, dict) and s.get("timeframe") == "1H" and s.get("status") == "PASS" for s in stages)
            if h1:
                full_pass.append(sym)
            else:
                near_miss.append(sym)
        except Exception:
            pass
    return sorted(set(full_pass)), sorted(set(near_miss))


def _screening_progress() -> dict:
    if not _QUEUE_FILE.exists():
        return {"total": 0, "screened": 0, "remaining": 0, "running_batches": 0}
    q = json.load(open(_QUEUE_FILE))
    symbols = q.get("queue", [])
    total = len(symbols)
    screened_set = set()
    for result_json in glob.glob(str(_EVIDENCE_BASE / "universe_screen*/results/*.json")):
        screened_set.add(Path(result_json).stem)
    screened = len([s for s in symbols if s in screened_set])
    running = sum(
        1 for pre in glob.glob(str(_EVIDENCE_BASE / "universe_screen*/pre_manifest.json"))
        if not Path(pre).parent.joinpath("summary.json").exists()
    )
    return {"total": total, "screened": screened, "remaining": total - screened, "running_batches": running}


def _last_shadow_run() -> dict:
    """Return info about the most recent shadow execution."""
    logs = sorted(_LOGS_DIR.glob("shadow_execution_*.log"), key=lambda p: p.stat().st_mtime) if _LOGS_DIR.exists() else []
    if not logs:
        return {"found": False}
    latest = logs[-1]
    age_s = (datetime.now(timezone.utc).timestamp() - latest.stat().st_mtime)
    try:
        content = latest.read_text(errors="ignore")
        placed = content.count('"placed"')
        skipped = content.count('"skipped"')
        errors = content.lower().count("error")
    except Exception:
        placed = skipped = errors = 0
    return {
        "found": True,
        "file": latest.name,
        "age_h": age_s / 3600,
        "errors": errors,
        "placed": placed,
        "skipped": skipped,
    }


def _shadow_orders_count() -> int:
    if not _SHADOW_ORDERS.exists():
        return 0
    try:
        return sum(1 for _ in open(_SHADOW_ORDERS))
    except Exception:
        return 0


# ─────────────────────────── formatters ─────────────────────────────────────

def _bar(n: int, total: int, width: int = 20) -> str:
    filled = int(n / total * width) if total else 0
    return "█" * filled + "░" * (width - filled)


def render_full() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "",
        "═" * 62,
        f"  OCTA Daily Status — {ts}",
        "═" * 62,
    ]

    # Processes
    lines += ["", "PROCESSES"]
    procs = _processes()
    labels = {"shadow_loop": "Shadow trading loop", "screening": "Universe screening", "paper_runner": "Paper runner"}
    for k, label in labels.items():
        mark = "✅" if procs[k] else "❌"
        lines.append(f"  {mark} {label}")

    # Registry
    lines += ["", "REGISTRY (artifacts/registry.sqlite3)"]
    recs = _registry_state()
    if recs:
        for r in recs:
            mark = "🟢" if r["status"] == "PAPER" else "🔵" if r["status"] == "SHADOW" else "⚫"
            lines.append(f"  {mark} {r['symbol']:8} {r['timeframe']:5} [{r['status']}]")
    else:
        lines.append("  (empty)")

    # Candidates
    lines += ["", "SCREENING CANDIDATES"]
    full, near = _candidates()
    sp = _screening_progress()
    pct = int(sp["screened"] * 100 / sp["total"]) if sp["total"] else 0
    lines.append(f"  1D+1H PASS:  {len(full):3}  → {', '.join(full) or '(none)'}")
    lines.append(f"  1D-only:     {len(near):3}  (near-misses)")
    lines.append(f"  Progress:    {sp['screened']}/{sp['total']} [{_bar(sp['screened'], sp['total'])}] {pct}%")
    lines.append(f"  Remaining:   {sp['remaining']}  |  Running batches: {sp['running_batches']}")

    # Last shadow run
    lines += ["", "LAST SHADOW RUN"]
    sr = _last_shadow_run()
    if sr["found"]:
        age_str = f"{sr['age_h']:.1f}h ago"
        ok = "✅" if sr["errors"] == 0 else "⚠️ "
        lines.append(f"  {ok} {sr['file']}")
        lines.append(f"     age={age_str}  errors={sr['errors']}")
    else:
        lines.append("  (no shadow logs found)")
    lines.append(f"  Total shadow orders logged: {_shadow_orders_count()}")

    # Summary
    paper_count = sum(1 for r in recs if r["status"] == "PAPER")
    lines += [
        "",
        "═" * 62,
        f"  PAPER artifacts: {paper_count}",
        f"  Candidates ready: {len(full)}",
        f"  Screening: {'COMPLETE' if sp['remaining'] == 0 else str(sp['remaining']) + ' remaining'}",
        "═" * 62,
        "",
    ]
    return "\n".join(lines)


def render_compact() -> str:
    procs = _processes()
    sp = _screening_progress()
    full, _ = _candidates()
    recs = _registry_state()
    paper = [r for r in recs if r["status"] == "PAPER"]
    sr = _last_shadow_run()
    shadow_ok = sr["found"] and sr["errors"] == 0
    ts = datetime.now(timezone.utc).strftime("%H:%M UTC")

    return (
        f"[{ts}] "
        f"shadow={'✅' if procs['shadow_loop'] else '❌'}  "
        f"screen={sp['screened']}/{sp['total']}  "
        f"candidates={len(full)}  "
        f"paper={len(paper)}({','.join(r['symbol'] for r in paper) or 'none'})  "
        f"last_run={'✅' if shadow_ok else '❌'}"
    )


def _telegram_summary() -> str:
    sp = _screening_progress()
    full, near = _candidates()
    recs = _registry_state()
    paper = [r["symbol"] for r in recs if r["status"] == "PAPER"]
    sr = _last_shadow_run()
    pct = int(sp["screened"] * 100 / sp["total"]) if sp["total"] else 0

    lines = [
        f"OCTA Daily Status — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"Screening: {sp['screened']}/{sp['total']} ({pct}%) | {sp['remaining']} left",
        f"Candidates (1D+1H PASS): {len(full)} — {', '.join(full) or 'none yet'}",
        f"Near-misses (1D only): {len(near)}",
        f"PAPER registry: {', '.join(paper) or 'none'}",
    ]
    if sr["found"]:
        age = f"{sr['age_h']:.1f}h ago"
        lines.append(f"Last shadow run: {age} | errors={sr['errors']}")
    if sp["remaining"] == 0 and len(full) >= 1:
        lines.append("READY: Monday shadow launch available")
    return "\n".join(lines)


# ─────────────────────────── entry point ────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="OCTA daily status report")
    ap.add_argument("--telegram", action="store_true", help="Also send summary to Telegram")
    ap.add_argument("--compact", action="store_true", help="One-line compact output")
    args = ap.parse_args()

    if args.compact:
        print(render_compact())
        return 0

    print(render_full())

    if args.telegram:
        try:
            from octa.execution.notifier import ExecutionNotifier
            notifier = ExecutionNotifier(
                evidence_dir=_EVIDENCE_BASE / "daily_status",
                rate_limit_seconds=0,
            )
            msg = _telegram_summary()
            ok = notifier.emit_alert("daily_status", {"message": msg})
            print(f"Telegram: {'✅ sent' if ok else '⚠️  not sent (check OCTA_TELEGRAM_ENABLED + OCTA_TELEGRAM_BOT_TOKEN)'}")
        except Exception as e:
            print(f"Telegram: error — {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
