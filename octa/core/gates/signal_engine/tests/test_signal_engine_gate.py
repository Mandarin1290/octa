from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from octa.core.cascade.contracts import GateDecision
from octa.core.data.providers.in_memory import InMemoryOHLCVProvider
from octa.core.data.providers.ohlcv import OHLCVBar
from octa.core.gates.signal_engine.gate import (
    OHLCVSeries,
    SignalDataProvider,
    SignalGate,
    SignalGateConfig,
)


@dataclass
class StaticProvider(SignalDataProvider):
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
        ts = start + timedelta(hours=1 * idx)
        bars.append(
            OHLCVBar(ts=ts, open=price, high=price * 1.01, low=price * 0.99, close=price, volume=10_000)
        )
    return bars


def test_signal_gate_long_on_uptrend_breakout() -> None:
    returns = [0.002] * 220
    close = _series(100.0, returns)

    series = _make_ohlcv(close)
    provider = StaticProvider(series=series)
    gate = SignalGate(data_provider=provider)

    outcome = gate.evaluate(["AAA"])
    payload = outcome.artifacts["AAA"]
    assert outcome.decision == GateDecision.PASS
    assert payload["signal"]["direction"] == "LONG"
    assert payload["signal"]["confidence"] >= gate._config.confidence_threshold


def test_signal_gate_short_on_downtrend_breakdown() -> None:
    returns = [-0.002] * 220
    close = _series(100.0, returns)

    series = _make_ohlcv(close)
    provider = StaticProvider(series=series)
    gate = SignalGate(data_provider=provider)

    outcome = gate.evaluate(["BBB"])
    payload = outcome.artifacts["BBB"]
    assert outcome.decision == GateDecision.PASS
    assert payload["signal"]["direction"] == "SHORT"


def test_signal_gate_flat_on_chop() -> None:
    returns = [0.01, -0.01] * 120
    close = _series(100.0, returns)

    series = _make_ohlcv(close)
    provider = StaticProvider(series=series)
    config = SignalGateConfig(score_margin=0.3, confidence_threshold=0.7)
    gate = SignalGate(data_provider=provider, config=config)

    outcome = gate.evaluate(["CCC"])
    payload = outcome.artifacts["CCC"]
    assert payload["signal"]["direction"] == "FLAT"
    assert outcome.decision == GateDecision.FAIL


def test_signal_gate_fails_on_missing_data() -> None:
    close = _series(100.0, [0.001] * 10)
    series = _make_ohlcv(close)
    provider = StaticProvider(series=series)
    gate = SignalGate(data_provider=provider)

    outcome = gate.evaluate(["DDD"])
    payload = outcome.artifacts["DDD"]
    assert payload["quality_flags"]["missing_data"] is True
    assert outcome.decision == GateDecision.FAIL


def test_signal_gate_with_in_memory_provider() -> None:
    returns = [0.001] * 210
    close = _series(100.0, returns)

    provider = InMemoryOHLCVProvider()
    provider.set_bars("AAA", "1H", _bars_from_close(close))
    gate = SignalGate(ohlcv_provider=provider)

    outcome = gate.evaluate(["AAA"])
    payload = outcome.artifacts["AAA"]
    assert payload["signal"]["direction"] in {"LONG", "SHORT", "FLAT"}
