#!/usr/bin/env python3
"""Rebuild Futures 1D parquets from local intraday (1H/30M/5M) data.

Usage:
    python scripts/rebuild_futures_1d.py [--limit N] [--symbols SYM1,SYM2] [--dry-run]

No network access required.  Reads from raw/Futures_Parquet (intraday).
Corrupt 1D files are moved to raw/Futures_Parquet_corrupt/ before replacement.

Output:
    raw/Futures_Parquet/<SYMBOL>_1D.parquet  (replaced)
    octa/var/evidence/<EVDIR>/per_symbol_manifest.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Offline guard
if os.environ.get("OCTA_ALLOW_NET", "0") not in ("0", "", None):
    print("ERROR: OCTA_ALLOW_NET must be 0 for this script", file=sys.stderr)
    sys.exit(2)

# Add project root to path
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from octa.core.data.builders.futures_1d_regen import (
    build_symbol_1d,
    write_manifest_entry,
    _find_source_parquet,
    SPEC_VERSION,
)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Rebuild Futures 1D parquets from intraday data (offline, fail-closed)."
    )
    parser.add_argument("--futures-dir", default="raw/Futures_Parquet")
    parser.add_argument("--corrupt-dir", default="raw/Futures_Parquet_corrupt")
    parser.add_argument("--evidence-dir", default=None, help="Override evidence output directory")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--symbols", default=None, help="Comma-separated symbol list")
    parser.add_argument("--dry-run", action="store_true", help="Scan only; do not write output")
    args = parser.parse_args(argv)

    futures_dir = Path(args.futures_dir)
    corrupt_dir = Path(args.corrupt_dir)

    # Evidence directory
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    evdir = Path(args.evidence_dir) if args.evidence_dir else Path(f"octa/var/evidence/b6_futures_1d_regen_{ts}")
    evdir.mkdir(parents=True, exist_ok=True)
    manifest_path = evdir / "per_symbol_manifest.jsonl"

    # Discover symbols
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    else:
        all_files = sorted(futures_dir.glob("*_1D.parquet"))
        symbols = sorted(set(f.stem.rsplit("_", 1)[0] for f in all_files))

    if args.limit:
        symbols = symbols[: args.limit]

    print(f"Futures dir : {futures_dir}")
    print(f"Corrupt dir : {corrupt_dir}")
    print(f"Evidence    : {evdir}")
    print(f"Spec version: {SPEC_VERSION}")
    print(f"Symbols     : {len(symbols)}")
    print(f"Dry run     : {args.dry_run}")
    print()

    ok_count = 0
    blocked: list[dict] = []

    for sym in symbols:
        try:
            # Pre-check: find source
            src_path, src_tf = _find_source_parquet(sym, futures_dir)

            if args.dry_run:
                print(f"  DRY {sym}: would use {src_tf}")
                ok_count += 1
                continue

            manifest_entry = build_symbol_1d(
                sym,
                futures_dir=futures_dir,
                output_dir=futures_dir,
                corrupt_dir=corrupt_dir,
            )
            write_manifest_entry(manifest_path, manifest_entry)
            print(
                f"  OK  {sym}: {manifest_entry['source_tf']} → {manifest_entry['output_rows']} 1D bars"
                f"  [{manifest_entry['output_start'][:10]} … {manifest_entry['output_end'][:10]}]"
            )
            ok_count += 1

        except RuntimeError as exc:
            reason = str(exc)
            print(f"  BLOCK {sym}: {reason}", file=sys.stderr)
            entry = {
                "symbol": sym,
                "status": "BLOCKED",
                "reason": reason,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "spec_version": SPEC_VERSION,
            }
            write_manifest_entry(manifest_path, entry)
            blocked.append(entry)

    # Write summary
    summary = {
        "total": len(symbols),
        "ok": ok_count,
        "blocked": len(blocked),
        "dry_run": args.dry_run,
        "spec_version": SPEC_VERSION,
        "evidence_dir": str(evdir),
        "blocked_symbols": [b["symbol"] for b in blocked],
    }
    (evdir / "regen_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )

    print()
    print(f"=== RESULT: {ok_count}/{len(symbols)} OK, {len(blocked)} blocked ===")
    for b in blocked:
        print(f"  BLOCKED: {b['symbol']} — {b['reason']}")

    return 0 if len(blocked) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
