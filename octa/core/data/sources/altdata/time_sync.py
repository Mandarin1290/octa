from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class TimeWindow:
    symbol: str
    timeframe: str
    start_ts: pd.Timestamp
    end_ts: pd.Timestamp
    bar_close_ts_series: pd.DatetimeIndex
    tz: str


def infer_timeframe(index: pd.DatetimeIndex) -> str:
    try:
        if not isinstance(index, pd.DatetimeIndex) or len(index) < 2:
            return "1D"
        deltas = index.to_series().diff().dropna().dt.total_seconds().values
        if len(deltas) == 0:
            return "1D"
        med = float(pd.Series(deltas).median())
        if med >= 20 * 3600:
            return "1D"
        if med >= 50 * 60:
            return "1H"
        if med >= 20 * 60:
            return "30m"
        if med >= 4 * 60:
            return "5m"
        return "1m"
    except Exception:
        return "1D"


def derive_timewindow_from_bars(
    *,
    bars_df: pd.DataFrame,
    symbol: str,
    tz: str = "UTC",
    timeframe: Optional[str] = None,
) -> TimeWindow:
    if not isinstance(bars_df.index, pd.DatetimeIndex):
        raise ValueError("bars_df must have a DatetimeIndex")
    idx = bars_df.index
    if idx.tz is None:
        idx = idx.tz_localize(tz)
    idx = idx.sort_values()
    tf = timeframe or infer_timeframe(idx)
    return TimeWindow(
        symbol=str(symbol or "unknown"),
        timeframe=str(tf),
        start_ts=pd.Timestamp(idx.min()),
        end_ts=pd.Timestamp(idx.max()),
        bar_close_ts_series=pd.DatetimeIndex(idx),
        tz=str(tz or "UTC"),
    )


def asof_join(
    *,
    bars_df: pd.DataFrame,
    alt_df: pd.DataFrame,
    on: str = "ts",
    tolerance: Optional[pd.Timedelta] = None,
    direction: str = "backward",
) -> pd.DataFrame:
    """Backward as-of merge onto bar index.

    `alt_df` must have a datetime column named `on`.
    Result index equals bars_df.index.
    """

    if not isinstance(bars_df.index, pd.DatetimeIndex):
        raise ValueError("bars_df must have a DatetimeIndex")
    if on not in alt_df.columns:
        raise ValueError(f"alt_df must contain column '{on}'")
    if direction != "backward":
        raise ValueError("Only backward direction is allowed")

    b = bars_df.copy()
    b = b.sort_index()
    b_on = pd.DataFrame({"bar_ts": b.index})

    a = alt_df.copy()
    a[on] = pd.to_datetime(a[on], utc=True, errors="coerce")
    a = a.dropna(subset=[on]).sort_values(on)

    merged = pd.merge_asof(
        b_on,
        a,
        left_on="bar_ts",
        right_on=on,
        direction="backward",
        tolerance=tolerance,
    )
    merged = merged.set_index("bar_ts")
    merged.index = pd.DatetimeIndex(merged.index)
    # Keep original index (including tz if any)
    merged = merged.reindex(b.index)
    return merged


def validate_no_future_leakage(
    *,
    merged_df: pd.DataFrame,
    bar_index: pd.DatetimeIndex,
    alt_ts_col: str = "ts",
    strict: bool = False,
) -> pd.Series:
    """Return boolean mask where leakage is detected (alt_ts > bar_ts).

    If strict: leakage rows are flagged; caller can drop/NaN features.
    """

    if alt_ts_col not in merged_df.columns:
        # if alt timestamp is absent, treat as no leakage (nothing joined)
        return pd.Series(False, index=bar_index)
    alt_ts = pd.to_datetime(merged_df[alt_ts_col], utc=True, errors="coerce")
    bar_ts = pd.to_datetime(bar_index, utc=True, errors="coerce")
    leak = (alt_ts.notna()) & (bar_ts.notna()) & (alt_ts > bar_ts)
    leak = pd.Series(leak.values, index=bar_index)
    if strict:
        return leak
    return leak
