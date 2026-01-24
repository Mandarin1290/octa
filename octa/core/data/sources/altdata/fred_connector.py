from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import pandas as pd


@dataclass
class FredFetchResult:
    series_id: str
    df: pd.DataFrame
    ok: bool
    error: Optional[str]


def fetch_fred_series(
    *,
    series_id: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    api_key: Optional[str] = None,
) -> FredFetchResult:
    """Fetch a FRED series for the given time window.

    Returns a DataFrame with columns: ts, value, as_of, source_time, ingested_at.
    For FRED, `as_of` is conservative: equals observation timestamp.
    """

    try:
        from fredapi import Fred  # type: ignore
    except Exception as e:
        return FredFetchResult(series_id=series_id, df=pd.DataFrame(), ok=False, error=f"fredapi missing: {e}")

    try:
        if api_key is None:
            return FredFetchResult(series_id=series_id, df=pd.DataFrame(), ok=False, error="missing api_key")

        fred = Fred(api_key=api_key)
        s = fred.get_series(series_id, start_ts.strftime("%Y-%m-%d"), end_ts.strftime("%Y-%m-%d"))
        if s is None or len(s) == 0:
            return FredFetchResult(series_id=series_id, df=pd.DataFrame(), ok=True, error=None)
        s.index = pd.to_datetime(s.index, utc=True, errors="coerce")
        s = s.dropna()
        df = pd.DataFrame({"ts": s.index, "value": pd.to_numeric(s.values, errors="coerce")})
        df = df.dropna(subset=["ts"])
        df["as_of"] = df["ts"]
        df["source_time"] = df["ts"]
        df["ingested_at"] = pd.Timestamp(datetime.utcnow(), tz="UTC")
        return FredFetchResult(series_id=series_id, df=df, ok=True, error=None)
    except Exception as e:
        return FredFetchResult(series_id=series_id, df=pd.DataFrame(), ok=False, error=str(e))


def fred_to_wide(
    *,
    series: List[FredFetchResult],
) -> pd.DataFrame:
    """Convert multiple FRED series results into a wide DF indexed by ts."""
    frames = []
    for r in series:
        if not r.ok or r.df is None or r.df.empty:
            continue
        x = r.df[["ts", "value"]].copy()
        x = x.rename(columns={"value": f"fred_{r.series_id}"})
        x = x.dropna(subset=["ts"])
        x["ts"] = pd.to_datetime(x["ts"], utc=True, errors="coerce")
        x = x.dropna(subset=["ts"]).sort_values("ts")
        x = x.set_index("ts")
        frames.append(x)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, axis=1)
    out = out.sort_index()
    return out
