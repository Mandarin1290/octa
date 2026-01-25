from datetime import datetime, timedelta, timezone

from octa.core.data.providers.ohlcv import OHLCVBar, OHLCVProvider
from octa.core.data.sources.altdata.orchestrator import build_altdata_stack
from octa.core.features.altdata.registry import FeatureRegistry
from octa.core.features.altdata import event_features, flow_features, macro_features
from octa.core.orchestration.adapters.l2_signal import L2SignalAdapter


class StaticProvider(OHLCVProvider):
    def __init__(self, bars):
        self._bars = bars

    def get_ohlcv(self, symbol, timeframe, start=None, end=None, limit=None):
        return self._bars


def _make_hourly_bars(n: int = 240):
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    bars = []
    price = 100.0
    for i in range(n):
        price += 0.1
        bars.append(
            OHLCVBar(
                ts=start + timedelta(hours=i),
                open=price - 0.2,
                high=price + 0.2,
                low=price - 0.4,
                close=price,
                volume=10000.0,
            )
        )
    return bars


def test_registry_asof_filters_future(tmp_path) -> None:
    reg = FeatureRegistry(run_id="test_run", root=str(tmp_path))
    t0 = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()
    t1 = datetime(2020, 1, 2, tzinfo=timezone.utc).isoformat()
    reg.write_market_features(
        timeframe="1D",
        gate_layer="global_1d",
        features={"macro_risk_score": 0.1},
        asof_ts=t0,
    )
    reg.write_market_features(
        timeframe="1D",
        gate_layer="global_1d",
        features={"macro_risk_score": 0.9},
        asof_ts=t1,
    )

    v0 = reg.get_market_feature_vector(timeframe="1D", gate_layer="global_1d", asof_ts=t0)
    v1 = reg.get_market_feature_vector(timeframe="1D", gate_layer="global_1d", asof_ts=t1)

    assert v0.get("macro_risk_score") == 0.1
    assert v1.get("macro_risk_score") == 0.9


def test_stooq_asof_filters_future() -> None:
    payloads = {
        "stooq": {
            "rows": [
                {"proxy": "vix", "symbol": "vix", "ts": "2020-01-01", "close": 10.0},
                {"proxy": "vix", "symbol": "vix", "ts": "2020-01-05", "close": 30.0},
            ]
        }
    }
    features = macro_features.build(payloads, asof_ts="2020-01-03T00:00:00+00:00")
    assert features.get("proxy_vix_level") == 10.0


def test_cot_asof_filters_future() -> None:
    payloads = {
        "cot": {
            "rows": [
                {
                    "market_id": "es",
                    "market_name": "E-MINI S&P 500",
                    "report_date": "2020-01-03",
                    "release_ts": "2020-01-03T20:00:00+00:00",
                    "noncommercial_long": 20000,
                    "noncommercial_short": 15000,
                    "open_interest": 100000,
                },
                {
                    "market_id": "es",
                    "market_name": "E-MINI S&P 500",
                    "report_date": "2020-01-10",
                    "release_ts": "2020-01-10T20:00:00+00:00",
                    "noncommercial_long": 30000,
                    "noncommercial_short": 10000,
                    "open_interest": 100000,
                },
            ]
        }
    }
    features = flow_features.build(payloads, asof_ts="2020-01-06T00:00:00+00:00")
    assert features.get("cot_net_position_es") == 0.05


def test_gdelt_asof_filters_future() -> None:
    payloads = {
        "gdelt": {
            "rows": [
                {
                    "asof_date": "2020-01-01",
                    "window": "1d",
                    "query_id": "conflict",
                "metric": "volume_intensity",
                "value": 0.2,
                "release_ts": "2020-01-01T06:00:00+00:00",
                "meta": {},
            },
            {
                "asof_date": "2020-01-02",
                "window": "1d",
                "query_id": "conflict",
                "metric": "volume_intensity",
                "value": 0.9,
                "release_ts": "2020-01-02T06:00:00+00:00",
                "meta": {},
            },
            ]
        }
    }
    features = event_features.build(payloads, asof_ts="2020-01-01T23:00:00+00:00")
    assert features.get("event_risk_score") == 0.2


def test_l2_signal_uses_backward_asof(tmp_path) -> None:
    reg = FeatureRegistry(run_id="test_run", root=str(tmp_path))
    t0 = datetime(2020, 1, 2, tzinfo=timezone.utc).isoformat()
    t1 = datetime(2020, 1, 15, tzinfo=timezone.utc).isoformat()
    reg.write_symbol_features(
        timeframe="1H",
        gate_layer="signal_1h",
        features_by_symbol={"AAA": {"attention_hype": 0.1}},
        asof_ts=t0,
    )
    reg.write_symbol_features(
        timeframe="1H",
        gate_layer="signal_1h",
        features_by_symbol={"AAA": {"attention_hype": 0.9}},
        asof_ts=t1,
    )

    provider = StaticProvider(_make_hourly_bars())
    adapter = L2SignalAdapter(provider=provider, feature_registry=reg)
    result = adapter.evaluate(symbol="AAA")

    assert result.payload["altdata_symbol"]["attention_hype"] == 0.1


def test_orchestrator_requires_asof(monkeypatch, tmp_path) -> None:
    cfg_path = tmp_path / "altdata.yaml"
    cfg_path.write_text("cache_dir: \"{root}\"\n".format(root=str(tmp_path)), encoding="utf-8")
    monkeypatch.setenv("OCTA_CONTEXT", "research")
    summary = build_altdata_stack(run_id="test_run", symbols=[], asof=None, allow_net=False, config_path=str(cfg_path))
    assert summary.get("status") == "missing_asof"
