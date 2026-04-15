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
        # Normalize series: accept list-of-strings OR list-of-dicts (with "id" key).
        # list-of-dicts format allows documenting feature_name/transform in the YAML
        # without changing the fetch logic.
        _raw_series = self.cfg.get("series") or []
        series = []
        for _s in _raw_series:
            if isinstance(_s, dict):
                _sid = str(_s.get("id") or "").strip()
            else:
                _sid = str(_s).strip()
            if _sid:
                series.append(_sid)
        if not series:
            return None
        # lookback_days: how many calendar days of history to fetch.
        # Default 4380 (~12 years) so snapshots cover full training period
        # (training uses bars from ~2015; 252-bar z_252 needs ≥1y of history).
        # Configurable via cfg.lookback_days for tests and special cases.
        lookback_days = int(self.cfg.get("lookback_days", 4380))
        start = asof - timedelta(days=lookback_days)
        payload: dict[str, Any] = {"series": {}}
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
