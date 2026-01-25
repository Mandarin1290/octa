from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Mapping

from octa.core.data.sources.altdata.fred_connector import fetch_fred_series


@dataclass
class FredSource:
    cfg: Mapping[str, Any]
    name: str = "fred"

    def __post_init__(self) -> None:
        self.enabled = bool(self.cfg.get("enabled", False))

    def cache_key(self, *, asof: date) -> str:
        return f"{self.name}_{asof.isoformat()}"

    def fetch_raw(self, *, asof: date, allow_net: bool) -> Mapping[str, Any] | None:
        if not allow_net:
            return None
        api_key = _get_env(self.cfg)
        if not api_key:
            return None
        series = self.cfg.get("series") or []
        series = [str(s).strip() for s in series if str(s).strip()]
        if not series:
            return None
        start = asof - timedelta(days=365)
        payload = {"series": {}}
        for series_id in series:
            res = fetch_fred_series(series_id=series_id, start_ts=start, end_ts=asof, api_key=api_key)
            if res.ok and res.df is not None:
                rows = []
                for _, row in res.df.iterrows():
                    rows.append({"ts": str(row.get("ts")), "value": row.get("value")})
                payload["series"][series_id] = rows
        return payload

    def normalize(self, raw: Mapping[str, Any]) -> Mapping[str, Any]:
        return raw


def _get_env(cfg: Mapping[str, Any]) -> str | None:
    key_env = str(cfg.get("api_key_env", "FRED_API_KEY"))
    import os

    return os.getenv(key_env)
