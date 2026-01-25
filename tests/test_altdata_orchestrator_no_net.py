from datetime import date

from octa.core.data.sources.altdata.cache import write_snapshot
from octa.core.data.sources.altdata.orchestrator import build_altdata_stack
from octa.core.features.altdata.registry import FeatureRegistry


def test_altdata_orchestrator_no_net(tmp_path) -> None:
    asof = date(2020, 1, 2)
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text(
        """
cache_dir: "{root}"
allow_net_default: false
sources:
  fred:
    enabled: true
    api_key_env: FRED_API_KEY
    series: ["FEDFUNDS"]
  gdelt:
    enabled: true
features_by_gate:
  global_1d:
    macro: true
    geopolitics: true
    positioning: false
    energy: false
""".format(root=str(tmp_path)),
        encoding="utf-8",
    )

    write_snapshot(
        source="fred",
        asof=asof,
        payload={"series": {"FEDFUNDS": [{"ts": "2020-01-01", "value": 2.0}]}} ,
        meta={"seed": True},
        root=str(tmp_path),
    )
    write_snapshot(
        source="gdelt",
        asof=asof,
        payload={"event_risk": 0.1},
        meta={"seed": True},
        root=str(tmp_path),
    )

    summary = build_altdata_stack(run_id="test_run", symbols=["AAA"], asof=asof, allow_net=False, config_path=str(cfg_path))
    reg = FeatureRegistry(run_id="test_run", root=str(tmp_path))
    market = reg.get_market_feature_vector(timeframe="1D", gate_layer="global_1d")

    assert summary["sources"]["fred"]["status"] == "ok"
    assert market.get("rates_fedfunds") == 2.0
