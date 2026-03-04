"""Timeseries integrity validation for OHLCV parquet data.

validate_timeseries_integrity(df, asset_class, timeframe, path)
  → (ok: bool, reason: str, details: dict)

Used as a pre-load gate in evaluate_data_quality() to detect corruption
patterns before the full load_parquet() pipeline.

Fail-closed contract:
  - Index not DatetimeIndex          → FUTURES_1D_CORRUPT_DATA:INDEX_NOT_DATETIME
  - Index contains price floats      → FUTURES_1D_CORRUPT_DATA:INDEX_CONTAINS_PRICE_FLOATS
  - Non-numeric OHLCV column         → FUTURES_1D_CORRUPT_DATA:NON_NUMERIC_COLUMN:<col>
  - Fewer than MIN_ROWS rows         → FUTURES_1D_CORRUPT_DATA:INSUFFICIENT_ROWS:<n>

No coerce heuristics.  No data repair.  Corrupt = block.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd

_MIN_ROWS: int = 20
# Plausible price range for the float-in-index detection heuristic.
_PRICE_RANGE_LO: float = 1e-4
_PRICE_RANGE_HI: float = 1_000_000.0


def validate_timeseries_integrity(
    df: pd.DataFrame,
    asset_class: str,
    timeframe: str,
    path: str,
) -> Tuple[bool, str, Dict[str, Any]]:
    """Check OHLCV dataframe structural integrity.

    Parameters
    ----------
    df:
        DataFrame as returned by ``pd.read_parquet()`` (raw, no normalization).
    asset_class, timeframe, path:
        Metadata recorded in the details dict for audit purposes.

    Returns
    -------
    (ok, reason, details)
        ok=True  → dataframe passes all checks; reason is empty string.
        ok=False → corruption detected; reason = ``FUTURES_1D_CORRUPT_DATA:<subreason>``.
    """
    details: Dict[str, Any] = {
        "path": str(path),
        "asset_class": str(asset_class),
        "timeframe": str(timeframe),
        "index_dtype": str(df.index.dtype),
        "index_name": str(df.index.name),
        "nrows": len(df),
    }

    # --- Check 1: index must be DatetimeIndex ---
    if not isinstance(df.index, pd.DatetimeIndex):
        idx_sample = [str(v) for v in list(df.index[:3])]
        details["index_sample"] = idx_sample

        # Detect the specific pattern: open-price floats stored as strings in
        # a column named 'datetime' that was written as the parquet index.
        price_float_detected = False
        if str(df.index.dtype) == "object" and idx_sample:
            try:
                sample_floats = [float(v) for v in idx_sample]
                if all(_PRICE_RANGE_LO <= f <= _PRICE_RANGE_HI for f in sample_floats):
                    price_float_detected = True
                    details["index_contains_price_floats"] = True
            except (ValueError, TypeError):
                pass

        subreason = (
            "INDEX_CONTAINS_PRICE_FLOATS" if price_float_detected else "INDEX_NOT_DATETIME"
        )
        details["subreason"] = subreason
        return False, f"FUTURES_1D_CORRUPT_DATA:{subreason}", details

    # --- Check 2: minimum rows ---
    if len(df) < _MIN_ROWS:
        subreason = f"INSUFFICIENT_ROWS:{len(df)}"
        details["subreason"] = subreason
        return False, f"FUTURES_1D_CORRUPT_DATA:{subreason}", details

    # --- Check 3: OHLCV columns must be numeric ---
    cols_lower = [str(c).lower() for c in df.columns]
    for col in ("open", "high", "low", "close", "volume"):
        try:
            idx = cols_lower.index(col)
        except ValueError:
            continue
        actual_col = list(df.columns)[idx]
        if not pd.api.types.is_numeric_dtype(df[actual_col]):
            subreason = f"NON_NUMERIC_COLUMN:{col}"
            details["subreason"] = subreason
            details["offending_dtype"] = str(df[actual_col].dtype)
            return False, f"FUTURES_1D_CORRUPT_DATA:{subreason}", details

    details["subreason"] = None
    return True, "", details


def write_quarantine_entry(
    quarantine_dir: Path,
    path: str,
    reason: str,
    asset_class: str,
    timeframe: str,
) -> None:
    """Append one entry to ``quarantine_manifest.jsonl`` (append-only).

    Parameters
    ----------
    quarantine_dir:
        Directory for the manifest file.  Created if it does not exist.
    path:
        Absolute or relative path of the quarantined file.
    reason:
        Full reason string, e.g. ``FUTURES_1D_CORRUPT_DATA:INDEX_CONTAINS_PRICE_FLOATS``.
    asset_class, timeframe:
        Metadata for the audit record.
    """
    quarantine_dir = Path(quarantine_dir)
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    sha = ""
    try:
        h = hashlib.sha256()
        with open(str(path), "rb") as fh:
            while True:
                chunk = fh.read(4 * 1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
        sha = h.hexdigest()
    except OSError:
        sha = ""

    entry = {
        "path": str(path),
        "sha256": sha,
        "reason": reason,
        "asset_class": str(asset_class),
        "timeframe": str(timeframe),
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest = quarantine_dir / "quarantine_manifest.jsonl"
    with manifest.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")
