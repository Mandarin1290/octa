from __future__ import annotations

import json
from datetime import date, datetime, timezone

from octa.core.data.sources.fundamentals.yahoo import (
    CorporateActions,
    EarningsEvents,
    FundamentalSnapshot,
    YahooHealth,
    build_yahoo_features,
    fetch_yahoo_fundamentals,
)


def _write_cache(tmp_path, symbol: str, payload: dict) -> None:
    asof = date.today().isoformat()
    base = tmp_path / "yahoo" / symbol / asof
    base.mkdir(parents=True, exist_ok=True)
    (base / "fundamentals.json").write_text(json.dumps(payload), encoding="utf-8")
    meta = {"fetched_at": datetime.now(timezone.utc).isoformat()}
    (base / "fundamentals_meta.json").write_text(json.dumps(meta), encoding="utf-8")


def test_fetch_yahoo_fundamentals_cache_hit(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_YAHOO_CACHE_ROOT", str(tmp_path / "yahoo"))
    monkeypatch.setenv("OCTA_YAHOO_AUDIT_ROOT", str(tmp_path / "audit"))
    monkeypatch.setenv("OCTA_ALLOW_NET", "0")

    symbol = "AAPL"
    payload = {"info": {"trailingPE": 12.0, "marketCap": 100.0}}
    _write_cache(tmp_path, symbol, payload)
    snap = fetch_yahoo_fundamentals(symbol)
    assert snap.health.cache_hit is True
    assert snap.data.get("info", {}).get("trailingPE") == 12.0


def test_build_yahoo_features_schema() -> None:
    health = YahooHealth(ok=True, cache_hit=True, errors=[], latency_ms=1.0, endpoint="fundamentals")
    snapshot = FundamentalSnapshot(symbol="AAPL", asof_date="2024-01-01", data={"info": {"trailingPE": 20.0}}, health=health)
    actions = CorporateActions(
        symbol="AAPL",
        asof_date="2024-01-01",
        dividends=[{"ts": "2024-01-01T00:00:00+00:00", "value": 0.2}],
        splits=[],
        health=health,
    )
    earnings = EarningsEvents(
        symbol="AAPL",
        asof_date="2024-01-01",
        events=[{"ts": "2099-01-10T00:00:00+00:00"}],
        health=health,
    )
    features = build_yahoo_features(snapshot, actions, earnings)
    assert "yahoo__pe_ttm" in features
    assert "yahoo__valuation_stretch" in features
    assert "yahoo__earnings_days_to" in features
