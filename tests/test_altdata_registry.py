from octa.core.features.altdata.registry import FeatureRegistry
from octa.core.features.altdata.builders import build_features


def test_altdata_registry_roundtrip(tmp_path) -> None:
    reg = FeatureRegistry(run_id="test_run", root=str(tmp_path))
    reg.write_market_features(timeframe="1D", gate_layer="global_1d", features={"macro_risk_score": 0.5})
    reg.write_symbol_features(timeframe="30M", gate_layer="structure_30m", features_by_symbol={"AAA": {"quality_score": 0.2}})

    market = reg.get_market_feature_vector(timeframe="1D", gate_layer="global_1d")
    symbol = reg.get_feature_vector(symbol="AAA", timeframe="30M", gate_layer="structure_30m")

    assert market.get("macro_risk_score") == 0.5
    assert symbol.get("quality_score") == 0.2


def test_gdelt_features_in_registry(tmp_path) -> None:
    reg = FeatureRegistry(run_id="test_run", root=str(tmp_path))
    payloads = {
        "gdelt": {
            "rows": [
                {
                    "asof_date": "2020-01-02",
                    "window": "1d",
                    "query_id": "conflict",
                    "metric": "volume",
                    "value": 0.4,
                    "release_ts": "2020-01-02T06:00:00+00:00",
                }
            ]
        }
    }
    build_features(
        run_id="test_run",
        timeframe="1D",
        gate_layer="global_1d",
        payloads=payloads,
        registry=reg,
        asof_ts="2020-01-02T23:00:00+00:00",
    )
    market = reg.get_market_feature_vector(timeframe="1D", gate_layer="global_1d", asof_ts="2020-01-02T23:00:00+00:00")
    assert market.get("event_risk_score") == 0.4
