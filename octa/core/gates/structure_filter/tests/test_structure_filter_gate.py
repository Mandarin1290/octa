from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from octa.core.cascade.contracts import GateDecision
from octa.core.data.providers.in_memory import InMemoryOHLCVProvider
from octa.core.data.providers.ohlcv import OHLCVBar
from octa.core.gates.structure_filter.gate import (
    OHLCVSeries,
    StructureDataProvider,
    StructureGate,
    StructureGateConfig,
)


@dataclass
class StaticProvider(StructureDataProvider):
    series: OHLCVSeries

    def get_ohlcv(self, symbol: str, timeframe: str) -> OHLCVSeries | None:
        return self.series


def _series(start: float, returns: list[float]) -> list[float]:
    prices = [start]
    for ret in returns:
        prices.append(prices[-1] * (1.0 + ret))
    return prices


def _make_ohlcv(close: list[float], vol: float = 10_000.0) -> OHLCVSeries:
    high = [price * 1.01 for price in close]
    low = [price * 0.99 for price in close]
    open_prices = close[:-1] + [close[-1]]
    volume = [vol for _ in close]
    return OHLCVSeries(open=open_prices, high=high, low=low, close=close, volume=volume)


def _bars_from_close(close: list[float]) -> list[OHLCVBar]:
    start = datetime(2024, 1, 1)
    bars: list[OHLCVBar] = []
    for idx, price in enumerate(close):
        ts = start + timedelta(minutes=30 * idx)
        bars.append(
            OHLCVBar(ts=ts, open=price, high=price * 1.01, low=price * 0.99, close=price, volume=10_000)
        )
    return bars


def test_structure_gate_passes_on_pullback_trend() -> None:
    returns = [0.002] * 120 + [-0.001, 0.001] * 50
    close = _series(100.0, returns)

    series = _make_ohlcv(close)
    provider = StaticProvider(series=series)
    gate = StructureGate(data_provider=provider)

    outcome = gate.evaluate(["AAA"])
    assert outcome.decision == GateDecision.PASS
    payload = outcome.artifacts["AAA"]
    assert payload["setup_zones"]
    assert payload["structure_metrics"]["trend_score"] != 0
    assert payload["quality_flags"]["missing_data"] is False


def test_structure_gate_fails_on_missing_data() -> None:
    close = _series(100.0, [0.001] * 20)
    series = _make_ohlcv(close)
    provider = StaticProvider(series=series)
    gate = StructureGate(data_provider=provider)

    outcome = gate.evaluate(["BBB"])
    assert outcome.decision == GateDecision.FAIL
    payload = outcome.artifacts["BBB"]
    assert payload["quality_flags"]["missing_data"] is True


def test_structure_gate_rejects_high_gap_risk() -> None:
    returns = [0.001] * 120 + [0.12] + [0.0] * 80
    close = _series(100.0, returns)
    series = _make_ohlcv(close)
    provider = StaticProvider(series=series)
    config = StructureGateConfig(gap_threshold=0.05)
    gate = StructureGate(data_provider=provider, config=config)

    outcome = gate.evaluate(["CCC"])
    payload = outcome.artifacts["CCC"]
    assert payload["quality_flags"]["gap_risk"] is True
    assert outcome.decision == GateDecision.FAIL


def test_structure_gate_with_in_memory_provider() -> None:
    returns = [0.002] * 210
    close = _series(100.0, returns)

    provider = InMemoryOHLCVProvider()
    provider.set_bars("AAA", "30M", _bars_from_close(close))
    gate = StructureGate(ohlcv_provider=provider)

    outcome = gate.evaluate(["AAA"])
    payload = outcome.artifacts["AAA"]
    assert payload["decision"] in {GateDecision.PASS.value, GateDecision.FAIL.value}
