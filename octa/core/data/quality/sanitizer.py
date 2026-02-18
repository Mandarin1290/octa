"""Deterministic data sanitization — flag-first, no silent clipping.

Checks:
  1. Duplicate timestamp detection + flagging
  2. Monotonic order enforcement
  3. Gap detection (missing expected periods)
  4. NaN detection (per-column)
  5. Spike detection (z-score based on rolling window)

Policy:
  - Each issue is FLAGGED with severity (INFO, WARNING, SEVERE).
  - SEVERE issues mark the symbol/tf as FAIL.
  - No data is silently clipped or modified.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

SEVERITY_INFO = "INFO"
SEVERITY_WARNING = "WARNING"
SEVERITY_SEVERE = "SEVERE"


@dataclass(frozen=True)
class SanitizationFlag:
    check: str
    severity: str
    message: str
    details: Dict[str, Any]


@dataclass(frozen=True)
class SanitizationResult:
    ok: bool
    symbol: str
    timeframe: str
    flags: list[SanitizationFlag]
    stats: Dict[str, Any]


def sanitize_series(
    df: pd.DataFrame,
    *,
    symbol: str = "UNKNOWN",
    timeframe: str = "UNKNOWN",
    close_col: str = "close",
    max_nan_frac: float = 0.20,
    spike_zscore_threshold: float = 5.0,
    spike_window: int = 20,
    expected_freq: Optional[str] = None,
    max_gap_frac: float = 0.10,
) -> SanitizationResult:
    """Run deterministic sanitization checks on a price series.

    Parameters
    ----------
    df : DataFrame
        Price data with DatetimeIndex.
    symbol, timeframe : str
        For labelling.
    close_col : str
        Column to check for spikes.
    max_nan_frac : float
        Max fraction of NaN before SEVERE.
    spike_zscore_threshold : float
        Z-score threshold for spike detection.
    spike_window : int
        Rolling window for spike z-score.
    expected_freq : str, optional
        Expected frequency (e.g. "1D", "1h") for gap detection.
    max_gap_frac : float
        Max fraction of expected periods that can be missing.
    """
    flags: List[SanitizationFlag] = []
    stats: Dict[str, Any] = {
        "rows_input": 0,
        "symbol": symbol,
        "timeframe": timeframe,
    }

    if not isinstance(df, pd.DataFrame) or len(df) == 0:
        flags.append(SanitizationFlag(
            check="empty_data", severity=SEVERITY_SEVERE,
            message="DataFrame is empty or not a DataFrame",
            details={},
        ))
        return SanitizationResult(ok=False, symbol=symbol, timeframe=timeframe, flags=flags, stats=stats)

    stats["rows_input"] = len(df)

    # 1. Duplicate timestamps
    if isinstance(df.index, pd.DatetimeIndex):
        dup_count = int(df.index.duplicated().sum())
        stats["duplicate_timestamps"] = dup_count
        if dup_count > 0:
            severity = SEVERITY_SEVERE if dup_count > 5 else SEVERITY_WARNING
            flags.append(SanitizationFlag(
                check="duplicate_timestamps", severity=severity,
                message=f"{dup_count} duplicate timestamps detected",
                details={"count": dup_count},
            ))
    else:
        flags.append(SanitizationFlag(
            check="non_datetime_index", severity=SEVERITY_SEVERE,
            message="Index is not a DatetimeIndex",
            details={"index_type": str(type(df.index).__name__)},
        ))

    # 2. Monotonic order
    if isinstance(df.index, pd.DatetimeIndex):
        is_monotonic = bool(df.index.is_monotonic_increasing)
        stats["index_monotonic"] = is_monotonic
        if not is_monotonic:
            flags.append(SanitizationFlag(
                check="non_monotonic_index", severity=SEVERITY_SEVERE,
                message="Index is not monotonically increasing",
                details={},
            ))

    # 3. NaN detection
    for col in df.columns:
        nan_frac = float(df[col].isna().mean())
        stats[f"nan_frac_{col}"] = round(nan_frac, 6)
        if nan_frac > max_nan_frac:
            flags.append(SanitizationFlag(
                check=f"nan_excessive_{col}", severity=SEVERITY_SEVERE,
                message=f"Column '{col}' has {nan_frac:.1%} NaN (threshold: {max_nan_frac:.1%})",
                details={"column": col, "nan_frac": round(nan_frac, 6), "threshold": max_nan_frac},
            ))
        elif nan_frac > 0:
            flags.append(SanitizationFlag(
                check=f"nan_present_{col}", severity=SEVERITY_INFO,
                message=f"Column '{col}' has {nan_frac:.1%} NaN",
                details={"column": col, "nan_frac": round(nan_frac, 6)},
            ))

    # 4. Gap detection
    if isinstance(df.index, pd.DatetimeIndex) and expected_freq and len(df) >= 2:
        try:
            expected_idx = pd.date_range(
                start=df.index[0], end=df.index[-1], freq=expected_freq
            )
            expected_count = len(expected_idx)
            actual_count = len(df)
            if expected_count > 0:
                gap_frac = 1.0 - (actual_count / expected_count)
                stats["gap_frac"] = round(max(0.0, gap_frac), 6)
                stats["expected_periods"] = expected_count
                stats["actual_periods"] = actual_count
                if gap_frac > max_gap_frac:
                    flags.append(SanitizationFlag(
                        check="excessive_gaps", severity=SEVERITY_WARNING,
                        message=f"Gap fraction {gap_frac:.1%} exceeds threshold {max_gap_frac:.1%}",
                        details={
                            "gap_frac": round(gap_frac, 6),
                            "threshold": max_gap_frac,
                            "expected": expected_count,
                            "actual": actual_count,
                        },
                    ))
        except Exception:
            pass

    # 5. Spike detection
    if close_col in df.columns and len(df) > spike_window:
        close = pd.to_numeric(df[close_col], errors="coerce")
        returns = close.pct_change().dropna()
        if len(returns) > spike_window:
            rolling_mean = returns.rolling(window=spike_window, min_periods=spike_window).mean()
            rolling_std = returns.rolling(window=spike_window, min_periods=spike_window).std()
            # Avoid division by zero
            safe_std = rolling_std.replace(0, np.nan)
            zscore = ((returns - rolling_mean) / safe_std).dropna()
            spike_count = int((zscore.abs() > spike_zscore_threshold).sum())
            stats["spike_count"] = spike_count
            stats["max_abs_zscore"] = round(float(zscore.abs().max()), 4) if len(zscore) > 0 else 0.0
            if spike_count > 0:
                severity = SEVERITY_SEVERE if spike_count > 5 else SEVERITY_WARNING
                flags.append(SanitizationFlag(
                    check="price_spikes", severity=severity,
                    message=f"{spike_count} price spikes detected (|z| > {spike_zscore_threshold})",
                    details={
                        "spike_count": spike_count,
                        "threshold": spike_zscore_threshold,
                        "max_abs_zscore": stats["max_abs_zscore"],
                    },
                ))

    # Determine overall pass/fail
    has_severe = any(f.severity == SEVERITY_SEVERE for f in flags)
    ok = not has_severe

    return SanitizationResult(
        ok=ok,
        symbol=symbol,
        timeframe=timeframe,
        flags=flags,
        stats=stats,
    )
