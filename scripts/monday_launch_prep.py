#!/usr/bin/env python3
"""Monday launch preparation: register canonically-passed PKL files, verify launch readiness.

Usage:
    python3 scripts/monday_launch_prep.py [--dry-run]

Scans canonical screening evidence for paper_ready=True symbols where the 1H stage
has status=PASS (not gate-failed models stored speculatively in paper_ready/).
Registers passing symbols into artifacts/registry.sqlite3 as PAPER status.
Prints a launch checklist.
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import pickle
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

REGISTRY_PATH = Path("artifacts/registry.sqlite3")
PAPER_READY_ROOT = Path("octa/var/models/paper_ready")
EVIDENCE_BASE = Path("octa/var/evidence")

# These had canonical walkforward/regime gate failures — do not auto-register
STRUCTURAL_FAILURES = {"ALB", "AWR", "AEM", "AIZ", "AON", "AVA", "AMZN"}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def get_canonical_passes() -> dict[str, dict]:
    """Return {symbol: {timeframe: metrics}} for symbols with 1D+1H PASS in evidence."""
    passes: dict[str, dict] = {}
    for result_json in sorted(glob.glob(str(EVIDENCE_BASE / "*/results/*.json"))):
        sym = Path(result_json).stem
        if sym in STRUCTURAL_FAILURES:
            continue
        try:
            d = json.load(open(result_json))
            if not d.get("paper_ready"):
                continue
            stages = d.get("stages", [])
            h1_pass = any(
                isinstance(s, dict) and s.get("timeframe") == "1H" and s.get("status") == "PASS"
                for s in stages
            )
            if not h1_pass:
                continue
            # Collect per-tf metrics
            tf_metrics: dict[str, dict] = {}
            for s in stages:
                if not isinstance(s, dict):
                    continue
                tf = s.get("timeframe")
                if tf and s.get("status") == "PASS":
                    m = s.get("metrics_summary") or s.get("metrics") or {}
                    tf_metrics[tf] = {
                        "sharpe": m.get("sharpe"),
                        "sortino": m.get("sortino"),
                        "oos_over_is": m.get("sharpe_oos_over_is"),
                        "max_dd": m.get("max_drawdown"),
                    }
            if sym not in passes or len(tf_metrics) > len(passes[sym]):
                passes[sym] = tf_metrics
        except Exception:
            pass
    return passes


def get_registry_state(db: sqlite3.Connection) -> dict[str, str]:
    """Return {symbol/timeframe: lifecycle_status}."""
    rows = db.execute("SELECT symbol, timeframe, lifecycle_status FROM artifacts").fetchall()
    return {f"{r[0]}/{r[1]}": r[2] for r in rows}


def register_artifact(
    db: sqlite3.Connection,
    symbol: str,
    timeframe: str,
    pkl: Path,
    meta_extra: dict,
    dry_run: bool,
) -> str:
    sha_path = pkl.with_suffix(".sha256")
    if sha_path.exists():
        actual_sha = sha_path.read_text().strip()
    else:
        actual_sha = sha256_file(pkl)
        if not dry_run:
            sha_path.write_text(actual_sha)

    with open(pkl, "rb") as f:
        obj = pickle.load(f)
    asset = obj.get("asset", {}) or {}
    run_id = obj.get("run_id") or obj.get("meta", {}).get("run_id") or "unknown"
    meta_json = json.dumps({
        "asset_class": asset.get("asset_class") or "stock",
        "run_id": run_id,
        "registered_at": datetime.now(timezone.utc).isoformat(),
        **meta_extra,
    })

    if not dry_run:
        db.execute(
            """
            INSERT OR REPLACE INTO artifacts
                (symbol, timeframe, lifecycle_status, path, sha256, meta_json)
            VALUES (?, ?, 'PAPER', ?, ?, ?)
            """,
            (symbol, timeframe, str(pkl), actual_sha, meta_json),
        )
        db.commit()
    return actual_sha[:12]


def main() -> int:
    parser = argparse.ArgumentParser(description="Monday launch prep: register canonically-passed PKLs")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    dry = args.dry_run
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*62}")
    print(f"  OCTA Monday Launch Prep — {ts}")
    print(f"  {'DRY RUN (no writes)' if dry else 'LIVE — will write to registry'}")
    print(f"{'='*62}\n")

    if not REGISTRY_PATH.exists():
        print(f"ERROR: Registry not found at {REGISTRY_PATH}")
        return 1

    db = sqlite3.connect(REGISTRY_PATH)
    registry = get_registry_state(db)

    # Find canonically passed symbols
    print("Scanning canonical evidence for 1D+1H PASS symbols...")
    passes = get_canonical_passes()
    if passes:
        print(f"  Found {len(passes)} symbol(s) with canonical 1H PASS:\n")
        for sym, tf_m in sorted(passes.items()):
            for tf, m in sorted(tf_m.items()):
                sh = f"{m['sharpe']:.3f}" if m.get("sharpe") else "N/A"
                print(f"    {sym:8} {tf:5} sharpe={sh}")
    else:
        print("  (none yet — screening still in progress)")
    print()

    # Register any not yet in registry as PAPER
    registered = []
    errors = []

    for sym, tf_metrics in sorted(passes.items()):
        for tf, m in sorted(tf_metrics.items()):
            key = f"{sym}/{tf}"
            if registry.get(key) == "PAPER":
                continue  # already registered

            # Find the PKL
            pkl = PAPER_READY_ROOT / sym / tf / f"{sym}.pkl"
            if not pkl.exists():
                # Try generic
                pkls = list((PAPER_READY_ROOT / sym / tf).glob("*.pkl")) if (PAPER_READY_ROOT / sym / tf).exists() else []
                if not pkls:
                    errors.append(f"{key}: PKL not found in paper_ready/")
                    continue
                pkl = pkls[0]

            # Verify it has safe_inference
            try:
                with open(pkl, "rb") as f:
                    obj = pickle.load(f)
                if "safe_inference" not in obj:
                    errors.append(f"{key}: not a tradeable artifact")
                    continue
            except Exception as e:
                errors.append(f"{key}: load error: {e}")
                continue

            sha_prefix = register_artifact(db, sym, tf, pkl, m, dry)
            sh = f"{m['sharpe']:.3f}" if m.get("sharpe") else "N/A"
            registered.append(f"{key}: sharpe={sh} sha={sha_prefix}")

    db.close()

    prefix = "[DRY RUN] " if dry else ""
    if registered:
        print(f"{prefix}Newly registered ({len(registered)}):")
        for r in registered:
            print(f"  ✅ {r}")
    else:
        print(f"{prefix}No new registrations needed (all already PAPER or no new passes).")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for e in errors:
            print(f"  ❌ {e}")

    # Final registry state
    db2 = sqlite3.connect(REGISTRY_PATH)
    paper_rows = db2.execute(
        "SELECT symbol, timeframe FROM artifacts WHERE lifecycle_status='PAPER' ORDER BY symbol, timeframe"
    ).fetchall()
    db2.close()

    print(f"\n{'='*62}")
    print(f"  PAPER artifacts after prep: {len(paper_rows)}")
    for r in paper_rows:
        print(f"    {r[0]:10} {r[1]}")
    print()

    if paper_rows:
        print(f"  ✅ READY FOR SHADOW LAUNCH")
        print(f"     Start: python3 scripts/run_shadow_execution.py")
        print(f"     Or:    bash /tmp/shadow_trading_loop.sh (full 48h)")
    else:
        print(f"  ⚠️  NO CANDIDATES YET — wait for screening to find 1H passes")

    print(f"{'='*62}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
