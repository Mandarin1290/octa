#!/usr/bin/env python3
"""
Generate 4H parquets by resampling 1H parquets for all Stock symbols.

Usage:
    python scripts/generate_4h_parquets.py [--root raw/Stock_parquet] [--dry-run] [--symbols AAON,MSFT]

Resample rules:
  - open:   first bar of 4H window
  - high:   max of all 1H bars in window
  - low:    min of all 1H bars in window
  - close:  last bar of 4H window
  - volume: sum of all 1H bars in window

4H boundary: UTC-aligned, offset 0h (00:00, 04:00, 08:00, 12:00, 16:00, 20:00).
Minimum output bars: 200 (symbols with fewer bars are skipped).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd


MIN_BARS = 200
RESAMPLE_RULE = "4h"
RESAMPLE_OFFSET = "0h"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resample_4h(df_1h: pd.DataFrame) -> pd.DataFrame:
    """Resample 1H OHLCV DataFrame to 4H."""
    ohlcv_cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df_1h.columns]
    agg = {}
    for col in ohlcv_cols:
        if col == "open":
            agg[col] = "first"
        elif col == "high":
            agg[col] = "max"
        elif col == "low":
            agg[col] = "min"
        elif col == "close":
            agg[col] = "last"
        elif col == "volume":
            agg[col] = "sum"
    extra_cols = [c for c in df_1h.columns if c not in ohlcv_cols]
    for col in extra_cols:
        agg[col] = "last"
    df_4h = df_1h.resample(RESAMPLE_RULE, offset=RESAMPLE_OFFSET).agg(agg)
    df_4h = df_4h.dropna(subset=[c for c in ["close"] if c in df_4h.columns])
    return df_4h


def _ensure_datetimeindex(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.index, pd.DatetimeIndex):
        for col in ["datetime", "date", "time", "timestamp"]:
            if col in df.columns:
                df = df.set_index(col)
                break
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def generate_for_symbol(path_1h: Path, output_dir: Path, *, dry_run: bool = False) -> dict:
    stem = path_1h.stem
    idx = stem.rfind("_")
    if idx <= 0:
        return {"symbol": stem, "status": "SKIP", "reason": "unparseable_stem"}
    symbol = stem[:idx].upper()
    tf = stem[idx + 1:].upper()
    if tf != "1H":
        return {"symbol": symbol, "status": "SKIP", "reason": f"not_1H:{tf}"}

    try:
        df = pd.read_parquet(path_1h)
        df = _ensure_datetimeindex(df)
        df = df.sort_index()
        df_4h = _resample_4h(df)
        if len(df_4h) < MIN_BARS:
            return {"symbol": symbol, "status": "SKIP", "reason": f"too_few_bars:{len(df_4h)}<{MIN_BARS}"}
        out_path = output_dir / f"{symbol}_4H.parquet"
        if not dry_run:
            df_4h.to_parquet(out_path, engine="pyarrow", index=True)
        return {"symbol": symbol, "status": "OK", "bars_1h": len(df), "bars_4h": len(df_4h), "path": str(out_path)}
    except Exception as exc:
        return {"symbol": symbol, "status": "ERROR", "reason": str(exc)[:200]}


def main() -> None:
    p = argparse.ArgumentParser(description="Generate 4H parquets from 1H parquets")
    p.add_argument("--root", default="raw/Stock_parquet", help="Directory containing *_1H.parquet files")
    p.add_argument("--out", default=None, help="Output directory (default: same as --root)")
    p.add_argument("--dry-run", action="store_true", default=False)
    p.add_argument("--symbols", default=None, help="Comma-separated symbol subset, e.g. AAON,MSFT")
    p.add_argument("--report", default=None, help="Write JSON report to this path")
    args = p.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"ERROR: root not found: {root}", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.out) if args.out else root
    output_dir.mkdir(parents=True, exist_ok=True)

    symbols_filter: Optional[set] = None
    if args.symbols:
        symbols_filter = {s.strip().upper() for s in args.symbols.split(",") if s.strip()}

    parquet_1h_files = sorted(root.glob("*_1H.parquet"))
    if symbols_filter:
        parquet_1h_files = [f for f in parquet_1h_files if f.stem.rsplit("_", 1)[0].upper() in symbols_filter]

    print(f"Found {len(parquet_1h_files)} 1H parquet files under {root}")
    if args.dry_run:
        print("DRY RUN — no files will be written")

    results = []
    ok = skip = error = 0
    for path_1h in parquet_1h_files:
        r = generate_for_symbol(path_1h, output_dir, dry_run=args.dry_run)
        results.append(r)
        status = r["status"]
        if status == "OK":
            ok += 1
            print(f"  [OK] {r['symbol']}: {r.get('bars_1h')} 1H bars → {r.get('bars_4h')} 4H bars")
        elif status == "SKIP":
            skip += 1
        else:
            error += 1
            print(f"  [ERROR] {r['symbol']}: {r.get('reason')}", file=sys.stderr)

    summary = {
        "timestamp": _utc_now_iso(),
        "root": str(root),
        "output_dir": str(output_dir),
        "dry_run": args.dry_run,
        "total": len(parquet_1h_files),
        "ok": ok,
        "skip": skip,
        "error": error,
    }
    print(f"\nSummary: {ok} OK, {skip} SKIP, {error} ERROR out of {len(parquet_1h_files)} symbols")

    if args.report:
        report = {"summary": summary, "results": results}
        Path(args.report).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Report written to {args.report}")


if __name__ == "__main__":
    import json
    main()
