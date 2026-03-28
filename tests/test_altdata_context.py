from __future__ import annotations

from datetime import datetime, timedelta, timezone

from octa.core.data.sources.altdata.cache import write_snapshot
from octa.core.features.altdata.registry import FeatureRegistry
from octa.core.features.altdata_context import AltDataContextBuilder


def test_altdata_context_freshness_filters(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_ALTDATA_LIVE_AUDIT_ROOT", str(tmp_path / "audit"))
    run_id = "altdata_ctx_test"
    registry = FeatureRegistry(run_id, root=str(tmp_path / "altdata"))

    asof_ts = datetime.now(timezone.utc).isoformat()
    registry.write_market_features(
        timeframe="1D",
        gate_layer="global_1d",
        features={
            "macro_risk_score": 0.4,
            "event_risk_score": 0.9,
            "news_risk_score": 0.7,
            "scheduled_event_bonus": 0.05,
            "cot_risk_score": 0.2,
        },
        asof_ts=asof_ts,
    )
    registry.write_symbol_features(
        timeframe="1H",
        gate_layer="signal_1h",
        features_by_symbol={"TEST": {"reddit_sentiment": 0.3, "reddit_volume": 0.2}},
        asof_ts=asof_ts,
    )

    asof_date = datetime.now(timezone.utc).date()
    fresh_meta = {"fetched_at": datetime.now(timezone.utc).isoformat()}
    stale_meta = {"fetched_at": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()}
    write_snapshot(source="fred", asof=asof_date, payload={"series": {}}, meta=fresh_meta, root=str(tmp_path / "altdata"))
    write_snapshot(source="cot", asof=asof_date, payload={"rows": []}, meta=fresh_meta, root=str(tmp_path / "altdata"))
    write_snapshot(source="reddit", asof=asof_date, payload={"sentiment": 0.2}, meta=fresh_meta, root=str(tmp_path / "altdata"))
    write_snapshot(source="news_events", asof=asof_date, payload={"rows": []}, meta=fresh_meta, root=str(tmp_path / "altdata"))
    write_snapshot(source="scheduled_events", asof=asof_date, payload={"rows": []}, meta=fresh_meta, root=str(tmp_path / "altdata"))
    write_snapshot(source="gdelt", asof=asof_date, payload={"rows": []}, meta=stale_meta, root=str(tmp_path / "altdata"))

    cfg = {
        "sources": {
            "fred": {"enabled": True, "cache_ttl_seconds": 86400},
            "cot": {"enabled": True, "cache_ttl_seconds": 86400},
            "reddit": {"enabled": True, "cache_ttl_seconds": 86400},
            "news_events": {"enabled": True, "cache_ttl_seconds": 86400},
            "scheduled_events": {"enabled": True, "cache_ttl_seconds": 86400},
            "gdelt": {"enabled": True, "cache_ttl_seconds": 3600},
        }
    }
    builder = AltDataContextBuilder(
        feature_registry=registry,
        config=cfg,
        cache_root=str(tmp_path / "altdata"),
        run_id=run_id,
    )
    context = builder.build(symbol="TEST", gate_layer="signal_1h", timeframe="1H", asof_ts=asof_ts)
    assert context.macro_regime_features.get("macro_risk_score") == 0.4
    assert context.event_stress_features.get("news_risk_score") == 0.7
    assert context.event_stress_features.get("scheduled_event_bonus") == 0.05
