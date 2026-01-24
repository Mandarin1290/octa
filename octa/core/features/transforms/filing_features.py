from __future__ import annotations

import pandas as pd


def build_filing_features(events: pd.DataFrame, *, ticker: str) -> pd.DataFrame:
    """Build conservative filing-event features.

    events: columns [ts, form, ticker] sorted by ts.
    Returns a DF indexed by ts with event markers.
    """
    if events is None or events.empty:
        return pd.DataFrame()
    e = events.copy()
    e = e[e.get("ticker", ticker) == ticker] if "ticker" in e.columns else e
    e["ts"] = pd.to_datetime(e["ts"], utc=True, errors="coerce")
    e = e.dropna(subset=["ts"]).sort_values("ts")
    out = pd.DataFrame(index=pd.DatetimeIndex(e["ts"]))
    form = e.get("form")
    if form is None:
        out["edgar_event"] = 1.0
        return out
    out["edgar_10k"] = (form.astype(str).str.upper() == "10-K").astype(float).values
    out["edgar_10q"] = (form.astype(str).str.upper() == "10-Q").astype(float).values
    out["edgar_8k"] = (form.astype(str).str.upper() == "8-K").astype(float).values
    out["edgar_event"] = 1.0
    return out
