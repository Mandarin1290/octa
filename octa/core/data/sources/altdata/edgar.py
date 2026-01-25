from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Mapping

from octa.core.data.sources.altdata.edgar_connector import fetch_edgar_filings


SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"


@dataclass
class EdgarSource:
    cfg: Mapping[str, Any]
    name: str = "edgar"

    def __post_init__(self) -> None:
        self.enabled = bool(self.cfg.get("enabled", False))

    def cache_key(self, *, asof: date) -> str:
        return f"{self.name}_{asof.isoformat()}"

    def fetch_raw(self, *, asof: date, allow_net: bool) -> Mapping[str, Any] | None:
        if not allow_net:
            return None
        user_agent = str(self.cfg.get("user_agent", "")).strip()
        if not user_agent:
            print("edgar: missing user_agent")
            return None

        ticker = str(self.cfg.get("symbol", "")).strip()
        if ticker:
            return _fetch_symbol_filings(ticker=ticker, asof=asof, cfg=self.cfg)
        return _fetch_ticker_map(user_agent=user_agent)

    def normalize(self, raw: Mapping[str, Any]) -> Mapping[str, Any]:
        return raw


def _fetch_symbol_filings(*, ticker: str, asof: date, cfg: Mapping[str, Any]) -> Mapping[str, Any] | None:
    forms = cfg.get("forms") or ["10-K", "10-Q", "8-K"]
    forms = [str(f).strip() for f in forms if str(f).strip()]
    start = asof - timedelta(days=365)
    res = fetch_edgar_filings(ticker=ticker, forms=forms, start_ts=start, end_ts=asof)
    if not res.ok or res.df is None:
        return None
    filings = []
    for _, row in res.df.iterrows():
        filings.append(
            {
                "cik": row.get("cik"),
                "form": row.get("form"),
                "filing_date": str(row.get("filing_date")),
            }
        )
    return {"symbol": ticker, "filings": filings}


def _fetch_ticker_map(*, user_agent: str) -> Mapping[str, Any] | None:
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
    }

    try:
        import httpx  # type: ignore

        with httpx.Client(timeout=10.0) as client:
            resp = client.get(SEC_TICKER_MAP_URL, headers=headers)
            if resp.status_code != 200:
                return _fallback_mapping()
            data = resp.json()
    except Exception:
        try:
            import urllib.request

            req = urllib.request.Request(SEC_TICKER_MAP_URL, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read().decode("utf-8")
            data = json.loads(raw)
        except Exception:
            return _fallback_mapping()

    rows = []
    if isinstance(data, dict):
        for _, entry in data.items():
            if not isinstance(entry, dict):
                continue
            ticker = str(entry.get("ticker", "")).strip().upper()
            cik = entry.get("cik_str")
            title = entry.get("title")
            if not ticker or cik is None:
                continue
            rows.append({"ticker": ticker, "cik": str(cik), "title": str(title or "")})

    if not rows:
        return _fallback_mapping()

    return {"mappings": rows}


def _fallback_mapping() -> Mapping[str, Any]:
    print("edgar: using fallback mapping")
    rows = [
        {"ticker": "AAPL", "cik": "320193", "title": "Apple Inc."},
        {"ticker": "MSFT", "cik": "789019", "title": "Microsoft Corp."},
    ]
    return {"mappings": rows}
