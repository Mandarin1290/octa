from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from octa.core.data.sources.altdata.cache import write_snapshot
from octa.core.data.sources.altdata.orchestrator import build_altdata_stack
from octa.core.features.altdata.registry import FeatureRegistry


def _seed_cache(asof) -> None:
    sample = {
        "series": {
            "FEDFUNDS": [{"ts": asof.isoformat(), "value": 5.0}],
            "DGS2": [{"ts": asof.isoformat(), "value": 4.0}],
            "DGS10": [{"ts": asof.isoformat(), "value": 4.5}],
            "DGS3MO": [{"ts": asof.isoformat(), "value": 4.2}],
            "CPIAUCSL": [{"ts": asof.isoformat(), "value": 300.0}],
        }
    }
    write_snapshot(source="fred", asof=asof, payload=sample, meta={"seed": True})
    gdelt_payload = {
        "status": "ok",
        "rows": [
            {
                "asof_date": asof.isoformat(),
                "window": "1d",
                "query_id": "conflict",
                "metric": "volume_intensity",
                "value": 0.2,
                "release_ts": asof.isoformat() + "T06:00:00+00:00",
                "meta": {"pack": "global_risk"},
            },
            {
                "asof_date": asof.isoformat(),
                "window": "7d",
                "query_id": "conflict",
                "metric": "volume_intensity",
                "value": 0.15,
                "release_ts": asof.isoformat() + "T06:00:00+00:00",
                "meta": {"pack": "global_risk"},
            },
        ]
    }
    write_snapshot(source="gdelt", asof=asof, payload=gdelt_payload, meta={"seed": True})
    _seed_cot_cache(asof)


def _seed_stooq_cache(asof) -> None:
    stooq_payload = {
        "rows": [
            {"proxy": "spx", "symbol": "spy.us", "ts": asof.isoformat(), "close": 500.0, "volume": 1000000},
            {"proxy": "vix", "symbol": "vix", "ts": asof.isoformat(), "close": 15.0, "volume": 0},
            {"proxy": "dxy", "symbol": "dxy", "ts": asof.isoformat(), "close": 100.0, "volume": 0},
            {"proxy": "gold", "symbol": "xauusd", "ts": asof.isoformat(), "close": 2000.0, "volume": 0},
            {"proxy": "oil", "symbol": "cl.f", "ts": asof.isoformat(), "close": 70.0, "volume": 0},
        ],
        "resolved": {"spx": "spy.us", "vix": "vix", "dxy": "dxy", "gold": "xauusd", "oil": "cl.f"},
    }
    write_snapshot(source="stooq", asof=asof, payload=stooq_payload, meta={"seed": True})


def _seed_cot_cache(asof) -> None:
    report_date = (asof - timedelta(days=7)).isoformat()
    release_ts = (asof - timedelta(days=4)).isoformat() + "T20:00:00+00:00"
    cot_payload = {
        "rows": [
            {
                "market_id": "es",
                "market_name": "E-MINI S&P 500",
                "report_date": report_date,
                "release_ts": release_ts,
                "noncommercial_long": 20000.0,
                "noncommercial_short": 15000.0,
                "open_interest": 100000.0,
            }
        ]
    }
    write_snapshot(source="cot", asof=asof, payload=cot_payload, meta={"seed": True})


@contextmanager
def _clear_proxy_env():
    keys = ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"]
    saved = {k: os.environ.get(k) for k in keys if k in os.environ}
    for key in keys:
        os.environ.pop(key, None)
    try:
        yield list(saved.keys())
    finally:
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value


def main() -> None:
    asof = datetime.now(timezone.utc).date()
    run_id = f"altdata_smoke_{asof.isoformat()}"
    allow_net = str(os.getenv("OCTA_ALLOW_NET", "")).strip() == "1"
    seed = str(os.getenv("OCTA_SMOKE_SEED", "1")).strip() != "0"
    if not allow_net and seed:
        _seed_cache(asof)
    if seed:
        _seed_stooq_cache(asof)
        _seed_cot_cache(asof)
    proxy_keys_present = []
    if allow_net:
        with _clear_proxy_env() as proxy_keys:
            proxy_keys_present = proxy_keys
            summary = build_altdata_stack(run_id=run_id, symbols=["TEST"], asof=asof, allow_net=allow_net)
    else:
        summary = build_altdata_stack(run_id=run_id, symbols=["TEST"], asof=asof, allow_net=allow_net)
    registry = FeatureRegistry(run_id)
    market = registry.get_market_feature_vector(timeframe="1D", gate_layer="global_1d")
    symbol = registry.get_feature_vector(symbol="TEST", timeframe="30M", gate_layer="structure_30m")
    print("run_id:", run_id)
    if allow_net:
        print("proxy_keys_present:", proxy_keys_present)
    print("summary_sources:", summary.get("sources"))
    print("market_features:", market)
    print("symbol_features:", symbol)


if __name__ == "__main__":
    main()
