#!/usr/bin/env python3
"""Convert index OHLC .txt files to Parquet in OCTA training schema.

These index files are typically CSV with 5 columns:
  timestamp,open,high,low,close

The training pipeline requires:
- a time column (timestamp/datetime/date/time)
- a 'close' column
- 'open/high/low' for most technical indicators
- 'volume' (used by VWAP/OBV/ADOSC)

This converter:
- parses timestamps as UTC
- normalizes columns to lowercase
- synthesizes 'volume' if missing (default 0.0)
- clamps high/low so that high>=max(open,close) and low<=min(open,close)
- writes one parquet per input file using a _1M suffix

Usage:
  python scripts/convert_index_txt_to_parquet.py \
      --src-dir raw/index_full_1min_n1q56ok \
      --dst-dir raw/index_1M
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


def _parse_name(path: Path) -> str:
    """Map '<BASE>_full_1min.txt' -> '<BASE>_1M'."""
    stem = path.stem  # e.g. SPX_full_1min
    lower = stem.lower()
    # common full_* patterns -> timeframe suffix mapping
    suffix_map = {
        '_full_1day': '_1D',
        '_full_1hour': '_1H',
        '_full_30min': '_30M',
        '_full_5min': '_5M',
        '_full_1min': '_1M',
    }
    for sfx, out in suffix_map.items():
        if lower.endswith(sfx):
            base = stem[: -len(sfx)]
            return f"{base}{out}"
    # fallback: detect minute/hour indicators
    if "1min" in lower and not stem.upper().endswith("_1M"):
        base = stem.replace("_1min", "").replace("_1MIN", "")
        return f"{base}_1M"
    if "1hour" in lower and not stem.upper().endswith("_1H"):
        base = stem.replace("_1hour", "")
        return f"{base}_1H"
    return stem


def _sniff_format(path: Path) -> Tuple[Optional[str], int]:
    """Return (sep, ncols_guess).

    sep=None means "use python engine sep inference".
    """
    line = ""
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        for _ in range(20):
            line = fh.readline()
            if not line:
                break
            line = line.strip()
            if line:
                break

    if not line:
        return ",", 0

    if "," in line:
        return ",", len(line.split(","))
    if "\t" in line:
        return "\t", len(line.split("\t"))
    # whitespace separated
    parts = line.split()
    if len(parts) >= 3:
        return r"\s+", len(parts)
    return None, 0


def _read_txt(path: Path) -> pd.DataFrame:
    sep, ncols = _sniff_format(path)

    # Common format observed: timestamp,open,high,low,close (no header)
    if ncols == 5:
        names = ["timestamp", "open", "high", "low", "close"]
    elif ncols >= 6:
        names = ["timestamp", "open", "high", "low", "close", "volume"] + [
            f"extra_{i}" for i in range(ncols - 6)
        ]
    else:
        names = None

    # Try no-header parse first (fast path)
    try:
        df = pd.read_csv(
            path,
            header=None,
            names=names,
            sep=sep,
            engine="python" if sep in (None, r"\s+") else "c",
        )
    except Exception:
        # Fallback: let pandas infer separator
        df = pd.read_csv(path, engine="python", sep=None)

    # normalize columns
    df.columns = [str(c).strip().lower() for c in df.columns]

    def _is_yyyymmdd_series(s: pd.Series) -> bool:
        try:
            ss = s.astype(str).str.strip()
            ss = ss[ss.notna() & (ss != "")]
            if ss.empty:
                return False
            # allow only digits and length 8
            sample = ss.iloc[:50]
            return bool(sample.str.fullmatch(r"\d{8}").all())
        except Exception:
            return False

    def _is_time_series(s: pd.Series) -> bool:
        try:
            ss = s.astype(str).str.strip()
            ss = ss[ss.notna() & (ss != "")]
            if ss.empty:
                return False
            sample = ss.iloc[:50]
            # HH:MM or HH:MM:SS
            return bool(sample.str.fullmatch(r"\d{1,2}:\d{2}(:\d{2})?").all())
        except Exception:
            return False

    def _parse_timestamp_only(col: pd.Series) -> pd.Series:
        # FX daily files use YYYYMMDD integers; parsing without format produces epoch-ns.
        if _is_yyyymmdd_series(col):
            return pd.to_datetime(col.astype(str), format="%Y%m%d", utc=True, errors="coerce")
        return pd.to_datetime(col, utc=True, errors="coerce")

    def _parse_timestamp_date_time(date_col: pd.Series, time_col: pd.Series) -> pd.Series:
        # FX intraday: YYYYMMDD,HH:MM:SS
        d = date_col.astype(str).str.strip()
        t = time_col.astype(str).str.strip()
        # normalize HH:MM -> HH:MM:00
        t = t.where(~t.str.fullmatch(r"\d{1,2}:\d{2}$"), t + ":00")
        joined = d + " " + t
        return pd.to_datetime(joined, format="%Y%m%d %H:%M:%S", utc=True, errors="coerce")

    # Special-case FX format detection when we used heuristic names.
    # If the file is actually: date,time,open,high,low,close,volume
    # the earlier name assignment is misaligned; fix it here.
    if "timestamp" in df.columns and "open" in df.columns and _is_yyyymmdd_series(df["timestamp"]) and _is_time_series(df["open"]):
        # Remap based on current names (timestamp=open=high=low=close=volume=extra_0)
        out = pd.DataFrame()
        out["timestamp"] = _parse_timestamp_date_time(df["timestamp"], df["open"])
        # Shift OHLCV one step to the right
        if "high" in df.columns:
            out["open"] = df["high"]
        if "low" in df.columns:
            out["high"] = df["low"]
        if "close" in df.columns:
            out["low"] = df["close"]
        if "volume" in df.columns:
            out["close"] = df["volume"]
        if "extra_0" in df.columns:
            out["volume"] = df["extra_0"]
        df = out
    else:
        # Generic path: find a time-like column and parse.
        cols = list(df.columns)
        time_col = None
        for cand in ("timestamp", "datetime", "date", "time"):
            if cand in cols:
                time_col = cand
                break
        if time_col is None:
            time_col = cols[0]

        df[time_col] = _parse_timestamp_only(df[time_col])
        df = df.rename(columns={time_col: "timestamp"})

    # keep only relevant OHLCV columns (ignore extras)
    keep = [c for c in ("timestamp", "open", "high", "low", "close", "volume") if c in df.columns]
    df = df[keep]

    # numeric conversions
    for c in ("open", "high", "low", "close", "volume"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # synthesize volume if missing
    if "volume" not in df.columns:
        df["volume"] = 0.0

    # drop invalid timestamps
    df = df.dropna(subset=["timestamp", "close"])

    # enforce monotonic time and dedupe
    df = df.sort_values("timestamp")
    df = df.drop_duplicates(subset=["timestamp"], keep="first")

    # clamp high/low consistency (helps octa_training.core.io_parquet validations)
    if all(c in df.columns for c in ("open", "high", "close")):
        df["high"] = df[["high", "open", "close"]].max(axis=1)
    if all(c in df.columns for c in ("open", "low", "close")):
        df["low"] = df[["low", "open", "close"]].min(axis=1)

    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--src-dir",
        default="raw/index_full_1min_n1q56ok",
        help="Directory containing index .txt files",
    )
    ap.add_argument(
        "--dst-dir",
        default="raw/index_1M",
        help="Output directory for parquet files",
    )
    ap.add_argument(
        "--pattern",
        default="*.txt",
        help="Glob pattern within src-dir (default: *.txt)",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="If >0, only convert first N files (for quick smoke tests)",
    )
    args = ap.parse_args()

    src_dir = Path(args.src_dir)
    dst_dir = Path(args.dst_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(src_dir.glob(args.pattern))
    if args.limit and args.limit > 0:
        files = files[: int(args.limit)]

    if not files:
        print(f"No files found in {src_dir} matching {args.pattern}")
        return 2

    wrote = 0
    failed = 0

    for f in files:
        try:
            df = _read_txt(f)
            out_name = _parse_name(f)
            out_path = dst_dir / f"{out_name}.parquet"
            df.to_parquet(out_path, index=False)
            wrote += 1
            if wrote <= 3:
                print(f"Wrote {out_path} rows={len(df)}")
        except Exception as e:
            failed += 1
            print(f"FAILED {f}: {e}")

    print(f"Done. wrote={wrote} failed={failed} out_dir={dst_dir}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
