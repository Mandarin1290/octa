from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import pandas as pd


@dataclass
class EdgarFetchResult:
    ok: bool
    df: pd.DataFrame
    error: Optional[str]


def fetch_edgar_filings(
    *,
    ticker: str,
    forms: List[str],
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> EdgarFetchResult:
    """Fetch EDGAR filings for a ticker within a time window.

    Safe-by-default implementation:
    - If downloader libs are missing, returns ok=False and empty DF.
    - Real ingestion can be enabled/extended without affecting callsites.

    Output columns (minimum):
      ticker, form, filing_date, accepted_datetime, as_of
    """
    try:
        # Prefer sec-edgar-downloader if available, but keep as optional.
        import sec_edgar_downloader  # type: ignore  # noqa: F401
    except Exception as e:
        return EdgarFetchResult(ok=False, df=pd.DataFrame(), error=f"edgar downloader missing: {e}")

    # Placeholder: return empty but OK (caller will treat as low coverage).
    df = pd.DataFrame(
        columns=["cik", "ticker", "form", "filing_date", "accepted_datetime", "doc_url", "raw_text_path", "as_of", "ingested_at"]
    )
    return EdgarFetchResult(ok=True, df=df, error=None)


def filings_to_events(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize filings to an event table with a single timestamp column `ts`.

    ts = accepted_datetime (preferred) else filing_date.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts", "form", "ticker"])
    out = df.copy()
    if "accepted_datetime" in out.columns:
        out["ts"] = pd.to_datetime(out["accepted_datetime"], utc=True, errors="coerce")
    elif "filing_date" in out.columns:
        out["ts"] = pd.to_datetime(out["filing_date"], utc=True, errors="coerce")
    else:
        out["ts"] = pd.NaT
    out = out.dropna(subset=["ts"])
    cols = [c for c in ["ts", "form", "ticker"] if c in out.columns]
    out = out[cols].sort_values("ts")
    return out
