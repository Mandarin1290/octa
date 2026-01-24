#!/usr/bin/env python3
"""Convert the 'US Indices' ZIP bundles (TXT OHLC files) into Parquet files compatible
with the OCTA training pipeline.

Input format (observed):
- One ZIP per timeframe
- Many files named like: AEX_full_1min.txt
- Each TXT is comma-separated with NO header:
    <timestamp>,<open>,<high>,<low>,<close>[,<volume>]
  where timestamp may be date-only (1day) or datetime (intraday).

Output format:
- One Parquet per source TXT.
- Columns: timestamp, open, high, low, close, volume (volume is filled with 0.0 if missing)
- Lowercase column names.

Example:
  python scripts/convert_us_indices_zip_to_parquet.py \
    --zip "raw/US Indices/index_full_1min_n1q56ok.zip"

By default output dir is derived as raw/<zip_stem>/.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from octa_training.core.io_parquet import sanitize_symbol


@dataclass
class ConvertStats:
    converted: int = 0
    skipped_existing: int = 0
    failed: int = 0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _looks_like_header(first_row: list[str]) -> bool:
    if not first_row:
        return False
    token0 = (first_row[0] or "").strip()
    # data rows start with 2008-01-02 or 2008-01-02 03:00:00
    if re.match(r"^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})?$", token0):
        return False
    # Anything else: assume header
    return True


def _detect_ncols_and_header(zf: zipfile.ZipFile, member: str) -> tuple[int, bool]:
    with zf.open(member, "r") as fh:
        # Read a small buffer (first line)
        raw = fh.readline()
        try:
            line = raw.decode("utf-8", errors="replace").strip("\r\n")
        except Exception:
            line = str(raw)
        # Parse with csv to respect quoting
        row = next(csv.reader([line], delimiter=","), [])
        ncols = len(row)
        has_header = _looks_like_header(row)
        return ncols, has_header


def _read_txt_from_zip(zf: zipfile.ZipFile, member: str) -> pd.DataFrame:
    ncols, has_header = _detect_ncols_and_header(zf, member)

    if ncols < 5:
        raise ValueError(f"unexpected column count {ncols} in {member}")

    if ncols >= 6:
        names = ["timestamp", "open", "high", "low", "close", "volume"]
        usecols = list(range(6))
    else:
        names = ["timestamp", "open", "high", "low", "close"]
        usecols = list(range(5))

    header = 0 if has_header else None

    with zf.open(member, "r") as fh:
        # pandas can read from a file-like; wrap in TextIO for decoding
        txt = io.TextIOWrapper(fh, encoding="utf-8", errors="replace", newline="")
        df = pd.read_csv(
            txt,
            sep=",",
            header=header,
            names=None if has_header else names,
            usecols=usecols,
            engine="python",
        )

    df.columns = [str(c).strip().lower() for c in df.columns]

    # Normalize timestamp
    if "timestamp" not in df.columns:
        # common alternatives
        for alt in ("datetime", "date", "time"):
            if alt in df.columns:
                df = df.rename(columns={alt: "timestamp"})
                break

    if "timestamp" not in df.columns:
        raise ValueError(f"missing timestamp column in {member}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).copy()

    # Coerce numeric columns
    for c in ("open", "high", "low", "close"):
        if c not in df.columns:
            raise ValueError(f"missing required column '{c}' in {member}")
        df[c] = pd.to_numeric(df[c], errors="coerce")

    if "volume" not in df.columns:
        df["volume"] = 0.0
    else:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)

    # Drop rows where price is NaN
    df = df.dropna(subset=["open", "high", "low", "close"]).copy()

    # Basic sanity for required price columns
    bad = (df[["open", "high", "low", "close"]] <= 0).any(axis=1)
    if bad.any():
        df = df.loc[~bad].copy()

    # Sort and de-dup
    df = df.sort_values("timestamp")
    df = df.drop_duplicates(subset=["timestamp"], keep="first")

    return df[["timestamp", "open", "high", "low", "close", "volume"]]


def _iter_members(zf: zipfile.ZipFile) -> Iterable[str]:
    for info in zf.infolist():
        if info.is_dir():
            continue
        name = info.filename
        if name.lower().endswith((".txt", ".csv")):
            yield name


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--zip", dest="zip_path", required=True, help="Path to a ZIP file")
    p.add_argument(
        "--out-dir",
        default=None,
        help="Output directory for parquet files. Default: raw/<zip_stem>/",
    )
    p.add_argument("--limit", type=int, default=0, help="Convert only N files (0=all)")
    p.add_argument("--force", action="store_true", help="Overwrite existing parquet")
    p.add_argument("--dry-run", action="store_true", help="Only print what would be done")
    args = p.parse_args()

    zip_path = Path(args.zip_path)
    if not zip_path.exists():
        raise SystemExit(f"ZIP not found: {zip_path}")

    out_dir = Path(args.out_dir) if args.out_dir else (REPO_ROOT / "raw" / zip_path.stem)
    out_dir.mkdir(parents=True, exist_ok=True)

    stats = ConvertStats()
    errors: list[dict] = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = list(_iter_members(zf))
        if args.limit and args.limit > 0:
            members = members[: args.limit]

        for m in members:
            stem = Path(m).stem
            symbol = sanitize_symbol(stem)
            out_path = out_dir / f"{symbol}.parquet"

            if out_path.exists() and not args.force:
                stats.skipped_existing += 1
                continue

            if args.dry_run:
                print(f"would_convert {m} -> {out_path}")
                continue

            try:
                df = _read_txt_from_zip(zf, m)
                if df.empty:
                    raise ValueError("empty dataframe after cleaning")
                df.to_parquet(out_path, index=False)
                stats.converted += 1
            except Exception as e:
                stats.failed += 1
                errors.append({"member": m, "error": str(e)})

    report = {
        "ts": _now_iso(),
        "zip": str(zip_path),
        "out_dir": str(out_dir),
        "stats": stats.__dict__,
        "errors": errors[:200],
    }
    report_path = out_dir / "_convert_report.json"
    if not args.dry_run:
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(report["stats"], ensure_ascii=False))
    if errors:
        print(f"wrote {report_path} (showing first {min(len(errors), 5)} errors)")
        for e in errors[:5]:
            print(f"- {e['member']}: {e['error']}")

    return 0 if stats.failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
