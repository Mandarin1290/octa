#!/usr/bin/env python3
"""Normalize Parquet files: ensure `timestamp` column exists, tz-aware UTC, sorted, deduped, and saved with index=False.

Usage: python3 scripts/normalize_parquets.py --dir raw/Indices_parquet --dry-run
"""
from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

import pandas as pd


def normalize_file(p: Path, dry_run: bool = False) -> dict:
    info = {"path": str(p)}
    try:
        df = pd.read_parquet(p)
    except Exception as e:
        info["error"] = f"read_error:{e}"
        return info

    # If index is datetime, move to column
    if isinstance(df.index, pd.DatetimeIndex) and ("timestamp" not in [c.lower() for c in df.columns]):
        df = df.reset_index()

    # find time column
    cols_l = {c.lower(): c for c in df.columns}
    time_col = None
    for cand in ("timestamp", "datetime", "date", "time"):
        if cand in cols_l:
            time_col = cols_l[cand]
            break
    if time_col is None:
        info["error"] = "missing_time_column"
        return info

    try:
        df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    except Exception as e:
        info["error"] = f"time_parse_error:{e}"
        return info

    # drop rows with NaT in timestamp
    n_before = len(df)
    df = df[~df[time_col].isna()]
    n_after = len(df)
    # sort by timestamp and dedupe
    df = df.sort_values(by=time_col)
    df = df[~df[time_col].duplicated(keep="first")]

    info.update({"rows_before": n_before, "rows_after": n_after, "time_col": time_col})

    if dry_run:
        return info

    # write to temp and move atomically
    tmp = Path(tempfile.mkdtemp()) / (p.name + ".tmp")
    try:
        df.to_parquet(tmp, index=False, compression="snappy")
        shutil.move(str(tmp), str(p))
        info["normalized"] = True
    except Exception as e:
        info["error"] = f"write_error:{e}"
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
    return info


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default="raw/Indices_parquet", help="Parquet directory to normalize")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--ext", default="parquet")
    args = p.parse_args(argv)

    d = Path(args.dir)
    if not d.exists():
        print("dir not found", d)
        return 2

    files = sorted(d.rglob(f"*.{args.ext}"))
    print(f"Found {len(files)} files in {d}")
    failures = 0
    for f in files:
        out = normalize_file(f, dry_run=args.dry_run)
        if out.get("error"):
            failures += 1
            print(f"FAIL {f}: {out.get('error')}")
        else:
            print(f"OK {f}: rows {out.get('rows_before')}->{out.get('rows_after')} normalized={out.get('normalized',False)}")

    print(f"Done. failures={failures}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
