"""
OCTA Top-50 Discovery Driver — Resume Script.

Resumes the top-50 cascade discovery run from position 45 (first unprocessed candidate).
Reads the current state from cascade_top50_queue.json and cascade_top50_master_log.json,
determines remaining candidates, runs each through the canonical control plane, and
updates all tracking files.

Usage:
    python scripts/resume_top50_discovery.py [--dry-run]

This script is deterministic, fail-closed, Foundation-scope only.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Canonical paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
QUEUE_FILE = ROOT / "cascade_top50_queue.json"
MASTER_LOG_FILE = ROOT / "cascade_top50_master_log.json"
STATUS_FILE = ROOT / "top50_driver_status.json"
CONFIG = "configs/foundation_validation.yaml"
PYTHON = sys.executable


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: object) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")


def _parse_stage_from_summary(summary_text: str, tf: str) -> str:
    """Extract stage status for a timeframe from training summary output."""
    pattern = rf"\[stage\].*?tf={tf}\s+status=(\w+)"
    m = re.search(pattern, summary_text)
    return m.group(1) if m else "UNKNOWN"


def _parse_gate_failure(summary_text: str) -> str:
    """Extract first gate failure reason from training summary output."""
    m = re.search(r"\[stage\].*?status=GATE_FAIL\s+reason=(.+?)(?:\s+pf=|$)", summary_text)
    if m:
        return m.group(1).strip()
    m2 = re.search(r"\[stage\].*?status=SKIP\s+reason=(.+?)(?:\s+pf=|$)", summary_text)
    return m2.group(1).strip() if m2 else "unknown"


def _cascade_depth(stages: dict) -> int:
    tfs = ["1D", "1H", "30M", "5M", "1M"]
    for i, tf in enumerate(reversed(tfs)):
        if stages.get(tf) not in (None, "SKIP"):
            return len(tfs) - i
    return 1


def run_candidate(
    symbol: str,
    asset_class: str,
    rank_score: float,
    attempt_number: int,
    dry_run: bool,
) -> dict:
    """Run a single candidate through the canonical Foundation training pipeline."""
    evidence_dir = f"/tmp/octa_top50_{attempt_number:02d}_{symbol}"
    t0 = time.time()
    print(f"\n[driver] attempt={attempt_number} symbol={symbol} asset_class={asset_class} score={rank_score:.6f}")
    print(f"[driver] evidence_dir={evidence_dir}")

    if dry_run:
        print(f"[driver] DRY_RUN — skipping actual training for {symbol}")
        stages = {"1D": "SKIP", "1H": "SKIP", "30M": "SKIP", "5M": "SKIP", "1M": "SKIP"}
        return {
            "attempt": attempt_number,
            "symbol": symbol,
            "asset_class": asset_class,
            "rank_score": rank_score,
            "evidence_dir": evidence_dir,
            "duration_sec": 0.0,
            "returncode": 0,
            "status": "dry_run",
            **stages,
            "walk_forward": False,
            "gate_failure_reason": "dry_run",
            "cascade_depth": 1,
            "summary_available": False,
            "result_available": False,
        }

    cmd = [
        PYTHON, "scripts/run_octa.py", "train",
        "--config", CONFIG,
        "--symbols", symbol,
        "--max-symbols", "1",
    ]
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=False,
        text=True,
        timeout=1200,
    )
    duration = time.time() - t0
    returncode = proc.returncode

    # Parse stage results from the evidence directory
    stages: dict[str, str] = {tf: "SKIP" for tf in ["1D", "1H", "30M", "5M", "1M"]}
    gate_failure_reason = "unknown"
    walk_forward = False
    cascade_depth_val = 1
    summary_available = False
    result_available = False

    try:
        # Find the result JSON for this symbol
        ev_root = ROOT / "octa" / "var" / "evidence"
        all_runs = sorted(
            [d for d in ev_root.iterdir() if d.is_dir() and d.name.startswith("full_cascade_")],
            reverse=True,
        )
        for run_dir in all_runs[:3]:
            result_file = run_dir / "results" / f"{symbol}.json"
            if result_file.exists():
                r = _load_json(result_file)
                for stage in r.get("stages", []):
                    tf = stage.get("timeframe", "")
                    st = stage.get("status", "SKIP")
                    if tf in stages:
                        stages[tf] = st
                if r.get("reason"):
                    gate_failure_reason = str(r["reason"])
                if r.get("status") == "PASS":
                    walk_forward = True
                result_available = True
                summary_available = True
                break
    except Exception as e:
        print(f"[driver] warn: could not parse result for {symbol}: {e}")

    # Derive cascade depth
    tfs_ordered = ["1D", "1H", "30M", "5M", "1M"]
    max_non_skip = 0
    for i, tf in enumerate(tfs_ordered):
        if stages.get(tf) not in (None, "SKIP"):
            max_non_skip = i + 1
    cascade_depth_val = max(1, max_non_skip)

    return {
        "attempt": attempt_number,
        "symbol": symbol,
        "asset_class": asset_class,
        "rank_score": rank_score,
        "evidence_dir": evidence_dir,
        "duration_sec": round(duration, 3),
        "returncode": returncode,
        "status": "completed",
        **stages,
        "walk_forward": walk_forward,
        "gate_failure_reason": gate_failure_reason,
        "cascade_depth": cascade_depth_val,
        "summary_available": summary_available,
        "result_available": result_available,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Resume OCTA top-50 discovery driver from last completed position.")
    ap.add_argument("--dry-run", action="store_true", help="Preflight only — no actual training")
    args = ap.parse_args()

    print(f"[driver] OCTA Top-50 Discovery Resume — {_utc_now()}")
    print(f"[driver] dry_run={args.dry_run}")

    # Load current state
    if not QUEUE_FILE.exists():
        print(f"[driver] FATAL: queue file not found: {QUEUE_FILE}")
        return 1
    if not MASTER_LOG_FILE.exists():
        print(f"[driver] FATAL: master log not found: {MASTER_LOG_FILE}")
        return 1

    queue = _load_json(QUEUE_FILE)
    master_log = _load_json(MASTER_LOG_FILE)

    all_candidates = queue.get("candidates", [])
    completed_attempts = master_log.get("attempts", [])
    completed_symbols = {a["symbol"] for a in completed_attempts}
    completed_count = len(completed_attempts)

    print(f"[driver] queue_size={len(all_candidates)} completed={completed_count} remaining={len(all_candidates) - completed_count}")

    remaining = [c for c in all_candidates if c["symbol"] not in completed_symbols]
    if not remaining:
        print("[driver] All candidates already completed. Nothing to do.")
        return 0

    print(f"[driver] Remaining candidates: {[c['symbol'] for c in remaining]}")

    new_results = []

    for i, candidate in enumerate(remaining):
        symbol = candidate["symbol"]
        asset_class = candidate.get("asset_class", "equities")
        rank_score = float(candidate.get("continuation_score", 0.0))
        attempt_num = completed_count + i + 1

        # Write driver status: show current candidate as running
        status_obj = {
            "queue_size": len(all_candidates),
            "completed_count": completed_count + i,
            "completed_candidates": master_log.get("attempts", []) + new_results,
            "running_candidate": {"symbol": symbol, "asset_class": asset_class},
            "pending_candidates": len(remaining) - i - 1,
        }
        _write_json(STATUS_FILE, status_obj)

        result = run_candidate(symbol, asset_class, rank_score, attempt_num, dry_run=args.dry_run)
        new_results.append(result)

        # Write per-attempt file
        attempt_file = ROOT / f"cascade_top50_attempt_{attempt_num:02d}.json"
        _write_json(attempt_file, result)
        print(f"[driver] attempt={attempt_num} {symbol} 1D={result['1D']} 1H={result['1H']} depth={result['cascade_depth']} t={result['duration_sec']:.1f}s")

        # Update master log after each symbol
        all_attempts_now = master_log.get("attempts", []) + new_results
        master_log_updated = {
            "attempt_count": len(all_attempts_now),
            "queue_size": len(all_candidates),
            "attempts": all_attempts_now,
            "completed_attempts": len(all_attempts_now),
            "active_reruns": [],
        }
        _write_json(MASTER_LOG_FILE, master_log_updated)

    # Final driver status: all done
    all_attempts_final = master_log.get("attempts", []) + new_results
    final_status = {
        "queue_size": len(all_candidates),
        "completed_count": len(all_attempts_final),
        "completed_candidates": all_attempts_final,
        "running_candidate": None,
        "pending_candidates": 0,
        "completed_at": _utc_now(),
    }
    _write_json(STATUS_FILE, final_status)

    print(f"\n[driver] Complete — {len(new_results)} new candidates processed")
    print(f"[driver] Total completed: {len(all_attempts_final)}/{len(all_candidates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
