#!/usr/bin/env python3
"""Convert external Split/Dividend adjusted stock TXT-in-ZIP dumps to Parquet.

Source format (observed): ZIP files containing many per-symbol .txt files:
- <SYMBOL>_full_1day_adjsplitdiv.txt   (rows: YYYY-MM-DD,open,high,low,close,volume)
- <SYMBOL>_full_1hour_adjsplitdiv.txt  (rows: YYYY-MM-DD HH:MM:SS,open,high,low,close,volume)
- <SYMBOL>_full_30min_adjsplitdiv.txt  (rows: YYYY-MM-DD HH:MM:SS,open,high,low,close,volume)
 - <SYMBOL>_full_5min_adjsplitdiv.txt   (rows: YYYY-MM-DD HH:MM:SS,open,high,low,close,volume)
 - <SYMBOL>_full_1min_adjsplitdiv.txt   (rows: YYYY-MM-DD HH:MM:SS,open,high,low,close,volume)
No header line.

Output:
- <out_dir>/<SANITIZED_SYMBOL>_1D.parquet for 1day
- <out_dir>/<SANITIZED_SYMBOL>_1H.parquet for 1hour
- <out_dir>/<SANITIZED_SYMBOL>_30M.parquet for 30min
 - <out_dir>/<SANITIZED_SYMBOL>_5M.parquet for 5min
 - <out_dir>/<SANITIZED_SYMBOL>_1M.parquet for 1min

Parquet schema is compatible with octa_training.core.io_parquet.load_parquet:
- columns lower-cased
- time column named 'timestamp'
- OHLC + volume

This script is intentionally idempotent by default (skips existing outputs unless --overwrite).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Iterable, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd

from octa_training.core.io_parquet import sanitize_symbol

_ZIP_NAME_RE = re.compile(
    r"stock_[A-Z]_full_(?P<tf>1day|1hour|30min|5min|1min)_adjsplitdiv_.*\.zip$",
    re.IGNORECASE,
)
_MEMBER_RE = re.compile(r"^(?P<sym>.+?)_full_(?P<tf>1day|1hour|30min|5min|1min)_adjsplitdiv\.txt$", re.IGNORECASE)


def _iter_zip_paths(source_dir: Path, *, tf_priority: dict[str, int]) -> Iterable[Path]:
    candidates: list[tuple[int, str, Path]] = []
    for p in source_dir.glob("*.zip"):
        m = _ZIP_NAME_RE.match(p.name)
        if not m:
            continue
        tf = (m.group("tf") or "").lower()
        prio = int(tf_priority.get(tf, 999))
        candidates.append((prio, p.name.lower(), p))

    for _, _, p in sorted(candidates):
        yield p


def _infer_tf_and_symbol(member_name: str) -> Optional[Tuple[str, str]]:
    m = _MEMBER_RE.match(member_name)
    if not m:
        return None
    sym_raw = m.group("sym")
    tf = m.group("tf").lower()
    sym = sanitize_symbol(sym_raw)
    return tf, sym


def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _write_member_to_parquet(
    *,
    zf,
    member_name: str,
    tf: str,
    symbol: str,
    out_path: Path,
    overwrite: bool,
    chunksize: int,
) -> str:
    if out_path.exists() and not overwrite:
        return "skipped_exists"

    _ensure_parent(out_path)

    # Use a unique temp file then atomic rename to avoid partial outputs.
    # (Prevents collisions if multiple converter instances run.)
    fd, tmp_name = tempfile.mkstemp(
        prefix=out_path.name + ".",
        suffix=f".tmp.{os.getpid()}",
        dir=str(out_path.parent),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)

    # Define schema
    col_names = ["timestamp", "open", "high", "low", "close", "volume"]
    parse_format = "%Y-%m-%d" if tf == "1day" else "%Y-%m-%d %H:%M:%S"

    import pyarrow as pa
    import pyarrow.parquet as pq

    writer = None
    rows_written = 0

    try:
        with zf.open(member_name, "r") as fh:
            # pandas can stream from file-like objects with chunksize.
            it = pd.read_csv(
                fh,
                header=None,
                names=col_names,
                chunksize=chunksize,
                dtype={
                    "open": "float64",
                    "high": "float64",
                    "low": "float64",
                    "close": "float64",
                    # Some symbols/timeframes contain non-integer or missing volumes.
                    # Read as float to avoid hard failures during bulk conversion.
                    "volume": "float64",
                },
                na_values=["", "NA", "NaN", "null"],
            )

            for chunk in it:
                if chunk is None or chunk.empty:
                    continue

                # Timestamp normalization
                ts = pd.to_datetime(chunk["timestamp"], format=parse_format, errors="coerce", utc=True)
                chunk["timestamp"] = ts
                chunk = chunk.dropna(subset=["timestamp", "close"])
                if chunk.empty:
                    continue

                # Ensure lower-case columns
                chunk.columns = [c.lower() for c in chunk.columns]

                table = pa.Table.from_pandas(chunk, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(str(tmp_path), table.schema, compression="zstd")
                writer.write_table(table)
                rows_written += int(len(chunk))

        if writer is not None:
            writer.close()
            writer = None

        # If nothing got written, fail closed so we don't create empty parquets.
        if rows_written <= 0:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            return "empty"

        tmp_path.replace(out_path)
        return "written"
    except Exception:
        try:
            if writer is not None:
                writer.close()
        except Exception:
            pass
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass
        raise


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--source-dir",
        default="/media/n-b/INTENSO/Stock/ Split-Dividend Adjusted",
        help="Directory containing stock_*.zip files (note the leading space in the default path).",
    )
    ap.add_argument(
        "--out-dir",
        default="raw/Stock_parquet",
        help="Output directory for Parquets.",
    )
    ap.add_argument(
        "--timeframes",
        default="1day,1hour,30min,5min,1min",
        help="Comma-separated list of timeframes to convert: 1day,1hour,30min,5min,1min",
    )
    ap.add_argument(
        "--symbols",
        default="",
        help="Optional comma-separated list of symbols to convert (unsanitized ok).",
    )
    ap.add_argument(
        "--symbols-file",
        default="",
        help="Optional path to a file with one symbol per line to convert (unsanitized ok).",
    )
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing parquet outputs.")
    ap.add_argument(
        "--chunksize",
        type=int,
        default=2_000_000,
        help="Rows per chunk for streaming conversion (1min files can be huge).",
    )
    args = ap.parse_args()

    source_dir = Path(args.source_dir)
    out_dir = Path(args.out_dir)

    tf_list = [x.strip().lower() for x in str(args.timeframes).split(",") if x.strip()]
    wanted_tfs = set(tf_list)
    allowed = {"1day", "1hour", "30min", "5min", "1min"}
    if not wanted_tfs.issubset(allowed):
        raise SystemExit(f"Invalid --timeframes={args.timeframes} (allowed: {','.join(sorted(allowed))})")

    wanted_syms = None
    syms_from_args: set[str] = set()
    if str(args.symbols).strip():
        syms_from_args |= {sanitize_symbol(s.strip()) for s in str(args.symbols).split(",") if s.strip()}
    if str(args.symbols_file).strip():
        sym_path = Path(str(args.symbols_file)).expanduser()
        if not sym_path.exists():
            raise SystemExit(f"symbols_file_missing: {sym_path}")
        lines = [ln.strip() for ln in sym_path.read_text().splitlines()]
        syms_from_args |= {sanitize_symbol(ln) for ln in lines if ln and not ln.startswith("#")}

    if syms_from_args:
        wanted_syms = syms_from_args

    if not source_dir.exists():
        print(f"source_dir_missing: {source_dir}", file=sys.stderr)
        return 2

    import zipfile

    counts = {"written": 0, "skipped_exists": 0, "empty": 0, "error": 0}

    tf_priority = {tf: i for i, tf in enumerate(tf_list)}
    for zip_path in _iter_zip_paths(source_dir, tf_priority=tf_priority):
        print(f"zip: {zip_path}")
        try:
            with zipfile.ZipFile(zip_path) as zf:
                for member in zf.namelist():
                    info = _infer_tf_and_symbol(member)
                    if not info:
                        continue
                    tf, sym = info
                    if tf not in wanted_tfs:
                        continue
                    if wanted_syms is not None and sym not in wanted_syms:
                        continue

                    out_tf = {"1day": "1D", "1hour": "1H", "30min": "30M", "5min": "5M", "1min": "1M"}.get(tf)
                    if not out_tf:
                        continue
                    out_path = out_dir / f"{sym}_{out_tf}.parquet"

                    try:
                        status = _write_member_to_parquet(
                            zf=zf,
                            member_name=member,
                            tf=tf,
                            symbol=sym,
                            out_path=out_path,
                            overwrite=bool(args.overwrite),
                            chunksize=int(args.chunksize),
                        )
                        counts[status] = counts.get(status, 0) + 1
                        if status in {"written", "empty"}:
                            print(f"  {sym} {tf} -> {out_path} [{status}]")
                    except Exception as e:
                        counts["error"] += 1
                        print(f"  {sym} {tf} -> {out_path} [error:{e}]", file=sys.stderr)
        except Exception as e:
            counts["error"] += 1
            print(f"zip_open_error: {zip_path} err={e}", file=sys.stderr)

    print(f"done: {counts}")
    return 0 if counts.get("error", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
