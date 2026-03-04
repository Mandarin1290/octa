"""Futures 1D dataset regeneration from local intraday parquets.

Specification (v1 — locked 2026-03-04):
  Input  : 1H parquet (preferred) → 30M → 5M
  Resample: UTC day boundary (00:00 UTC → 23:59:59.999 UTC)
             pd.Grouper(freq="1D", label="left", closed="left")
  OHLCV  : open=first, high=max, low=min, close=last, volume=sum
           (volume column omitted when not present in source)
  Index  : UTC DatetimeIndex (timezone-aware), monotonic increasing
  Missing: no forward-fill; missing UTC days are absent from output
  Filter : drop rows where close is NaN (empty day buckets)

Fail-closed:
  - No intraday source available  → raise RuntimeError("NO_INTRADAY_SOURCE")
  - After resample < 20 rows      → raise RuntimeError("INSUFFICIENT_ROWS_AFTER_RESAMPLE")
  - validate_timeseries_integrity fails after resample → raise RuntimeError(reason)
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from octa.core.data.io.timeseries_integrity import validate_timeseries_integrity

# Spec version tag — bump when aggregation rules change
SPEC_VERSION = "v1"

# TF priority order for source selection
_TF_PRIORITY: List[str] = ["1H", "30M", "5M"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(4 * 1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _find_source_parquet(
    symbol: str,
    futures_dir: Path,
) -> Tuple[Path, str]:
    """Return (path, timeframe) of the best available intraday source.

    Priority: 1H > 30M > 5M.

    Raises
    ------
    RuntimeError("NO_INTRADAY_SOURCE")
        If none of the preferred TF files exist.
    """
    for tf in _TF_PRIORITY:
        candidate = futures_dir / f"{symbol}_{tf}.parquet"
        if candidate.exists():
            return candidate, tf
    raise RuntimeError(f"NO_INTRADAY_SOURCE:{symbol}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_intraday(path: Path) -> pd.DataFrame:
    """Load an intraday parquet and return it with a UTC-aware DatetimeIndex.

    The source parquets have naive ``datetime64[ns]`` indexes that represent
    UTC timestamps (CME Globex data stored without tz annotation).  We
    localize to UTC here — no conversion, no coercion.

    Raises
    ------
    ValueError
        If the parquet has no recognisable datetime index or column.
    """
    df = pd.read_parquet(str(path))

    if isinstance(df.index, pd.DatetimeIndex):
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
    else:
        # Try to find a time column in reset-index form
        df_r = df.reset_index()
        time_candidates = [c for c in df_r.columns if c.lower() in ("datetime", "timestamp", "date", "time")]
        if not time_candidates:
            raise ValueError(f"No DatetimeIndex and no time column found in {path}")
        col = time_candidates[0]
        ts = pd.to_datetime(df_r[col], utc=True, errors="coerce")
        if ts.isna().any():
            raise ValueError(f"Unparseable timestamps in time column {col!r} of {path}")
        df_r[col] = ts
        df_r = df_r.set_index(col).sort_index()
        df = df_r

    df = df.sort_index()
    df = df[~df.index.duplicated(keep="first")]
    df.columns = [str(c).lower() for c in df.columns]
    return df


def resample_to_1d(df: pd.DataFrame) -> pd.DataFrame:
    """Resample an intraday OHLCV DataFrame to UTC 1-day bars.

    Spec (v1):
      - Boundaries: UTC calendar day (00:00 UTC, closed/label left)
      - open  = first non-NaN value
      - high  = max
      - low   = min
      - close = last non-NaN value
      - volume = sum (present only when 'volume' column exists)
      - Missing days: absent (no forward-fill)
    """
    agg: Dict[str, str] = {}
    cols = list(df.columns)

    if "open" in cols:
        agg["open"] = "first"
    if "high" in cols:
        agg["high"] = "max"
    if "low" in cols:
        agg["low"] = "min"
    if "close" in cols:
        agg["close"] = "last"
    if "volume" in cols:
        agg["volume"] = "sum"

    df_1d = df.resample("1D", label="left", closed="left").agg(agg)

    # Drop rows where close is NaN (no intraday data in that UTC day bucket)
    if "close" in df_1d.columns:
        df_1d = df_1d.dropna(subset=["close"])

    df_1d = df_1d.sort_index()
    return df_1d


def build_symbol_1d(
    symbol: str,
    *,
    futures_dir: Path,
    output_dir: Path,
    quarantine_dir: Optional[Path] = None,
    corrupt_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """Build the 1D parquet for *symbol* from the best available intraday source.

    Steps:
    1. Find intraday source (1H > 30M > 5M)
    2. Load + localize to UTC
    3. Resample to 1D
    4. validate_timeseries_integrity (must PASS)
    5. Move existing 1D file to *corrupt_dir* (if given)
    6. Write new 1D parquet to *output_dir*

    Returns a manifest dict recording all hashes and metadata.

    Raises
    ------
    RuntimeError
        On any blocking condition (missing source, insufficient rows,
        integrity check failure).
    """
    futures_dir = Path(futures_dir)
    output_dir = Path(output_dir)

    manifest: Dict[str, Any] = {
        "symbol": symbol,
        "spec_version": SPEC_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "PENDING",
    }

    # 1. Find source
    src_path, src_tf = _find_source_parquet(symbol, futures_dir)
    manifest["source_tf"] = src_tf
    manifest["source_path"] = str(src_path)
    manifest["source_sha256"] = _sha256_file(src_path)

    # 2. Load intraday
    df_intraday = load_intraday(src_path)
    manifest["source_rows"] = len(df_intraday)
    manifest["source_start"] = str(df_intraday.index.min())
    manifest["source_end"] = str(df_intraday.index.max())

    # 3. Resample to 1D
    df_1d = resample_to_1d(df_intraday)
    if len(df_1d) < 20:
        raise RuntimeError(
            f"INSUFFICIENT_ROWS_AFTER_RESAMPLE:{symbol}:{len(df_1d)}"
        )

    # 4. Validate integrity
    ok, reason, _ = validate_timeseries_integrity(df_1d, "futures", "1D", str(output_dir / f"{symbol}_1D.parquet"))
    if not ok:
        raise RuntimeError(f"INTEGRITY_FAIL_AFTER_RESAMPLE:{symbol}:{reason}")

    manifest["output_rows"] = len(df_1d)
    manifest["output_start"] = str(df_1d.index.min())
    manifest["output_end"] = str(df_1d.index.max())

    # 5. Move existing corrupt 1D file if present
    existing_1d = futures_dir / f"{symbol}_1D.parquet"
    if existing_1d.exists():
        old_sha = _sha256_file(existing_1d)
        manifest["corrupt_sha256"] = old_sha
        if corrupt_dir is not None:
            corrupt_dir = Path(corrupt_dir)
            corrupt_dir.mkdir(parents=True, exist_ok=True)
            dest = corrupt_dir / existing_1d.name
            existing_1d.rename(dest)
            manifest["corrupt_moved_to"] = str(dest)
        # else: overwrite in place (output_dir == futures_dir path handled below)

    # 6. Write output
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{symbol}_1D.parquet"
    df_1d.to_parquet(str(out_path))
    manifest["output_path"] = str(out_path)
    manifest["output_sha256"] = _sha256_file(out_path)
    manifest["status"] = "OK"

    return manifest


def write_manifest_entry(manifest_path: Path, entry: Dict[str, Any]) -> None:
    """Append one manifest entry to *manifest_path* (JSONL, append-only)."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, default=str) + "\n")
