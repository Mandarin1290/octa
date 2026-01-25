from datetime import datetime, timedelta, timezone

from octa.core.data.providers.ohlcv import OHLCVBar, OHLCVProvider
from octa.core.features.altdata.registry import FeatureRegistry
from octa.core.orchestration.adapters.l3_structure import L3StructureAdapter


class StaticProvider(OHLCVProvider):
    def __init__(self, bars):
        self._bars = bars

    def get_ohlcv(self, symbol, timeframe, start=None, end=None, limit=None):
        return self._bars


def _make_bars(n: int = 250):
    now = datetime(2020, 1, 1)
    bars = []
    price = 100.0
    for i in range(n):
        price += 0.1
        bars.append(
            OHLCVBar(
                ts=now + timedelta(minutes=30 * i),
                open=price - 0.2,
                high=price + 0.2,
                low=price - 0.4,
                close=price,
                volume=10000.0,
            )
        )
    return bars


def test_structure_adapter_altdata_overlay(tmp_path):
    reg = FeatureRegistry(run_id="test_run", root=str(tmp_path))
    asof_ts = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()
    reg.write_symbol_features(
        timeframe="30M",
        gate_layer="structure_30m",
        features_by_symbol={"AAA": {"quality_score": 0.1}},
        asof_ts=asof_ts,
    )

    alt_cfg = {"overlays": {"enabled": True, "structure_30m": {"min_quality": 0.2}}}
    provider = StaticProvider(_make_bars())
    adapter = L3StructureAdapter(provider, feature_registry=reg, altdata_config=alt_cfg)
    result = adapter.evaluate(symbol="AAA")
    assert result.decision == "FAIL"
    assert result.reason == "altdata_quality_low"
