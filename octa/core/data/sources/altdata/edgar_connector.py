from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from octa.core.data.sources.altdata._compat_rate_limiter import _make_limiter, _patch_limiter_for_kwargs


@dataclass
class EdgarFetchResult:
    ok: bool
    df: pd.DataFrame
    error: Optional[str]
    meta: Optional[Dict[str, Any]] = None


def _short_err(err: Exception | str, limit: int = 200) -> str:
    msg = str(err)
    return msg[:limit] if len(msg) > limit else msg


def _import_downloader() -> Tuple[Optional[Any], Dict[str, Any]]:
    meta: Dict[str, Any] = {}
    try:
        import sec_edgar_downloader  # type: ignore  # noqa: F401

        return sec_edgar_downloader, meta
    except Exception as exc:
        msg = str(exc)
        if "raise_when_fail" in msg or "raise_on_fail" in msg:
            patch_meta = _patch_limiter_for_kwargs(raise_when_fail=False)
            meta.update(patch_meta)
            try:
                import importlib

                sec_edgar_downloader = importlib.import_module("sec_edgar_downloader")
                return sec_edgar_downloader, meta
            except Exception as exc2:
                meta["error"] = _short_err(exc2)
                return None, meta
        meta["error"] = _short_err(exc)
        return None, meta


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
    limiter, limiter_meta = _make_limiter(raise_when_fail=False)
    meta: Dict[str, Any] = {
        "limiter_impl": limiter_meta.get("limiter_impl"),
        "limiter_kwargs_used": limiter_meta.get("limiter_kwargs_used"),
    }
    if limiter is None:
        meta["limiter_error"] = limiter_meta.get("error")
        return EdgarFetchResult(ok=False, df=pd.DataFrame(), error="edgar limiter unavailable", meta=meta)

    downloader, import_meta = _import_downloader()
    meta.update(import_meta)
    if downloader is None:
        err = meta.get("error") or "edgar downloader missing"
        return EdgarFetchResult(ok=False, df=pd.DataFrame(), error=f"edgar downloader missing: {err}", meta=meta)

    # Placeholder: return empty but OK (caller will treat as low coverage).
    df = pd.DataFrame(
        columns=["cik", "ticker", "form", "filing_date", "accepted_datetime", "doc_url", "raw_text_path", "as_of", "ingested_at"]
    )
    return EdgarFetchResult(ok=True, df=df, error=None, meta=meta)


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
