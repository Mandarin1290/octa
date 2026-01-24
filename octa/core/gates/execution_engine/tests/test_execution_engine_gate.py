from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from octa.core.cascade.contracts import GateDecision
from octa.core.data.providers.in_memory import InMemoryOHLCVProvider
from octa.core.data.providers.ohlcv import OHLCVBar
from octa.core.gates.execution_engine.gate import (
    ExecutionDataProvider,
    ExecutionGate,
    ExecutionGateConfig,
    OHLCVSeries,
)


@dataclass
class StaticProvider(ExecutionDataProvider):
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
        ts = start + timedelta(minutes=5 * idx)
        bars.append(
            OHLCVBar(ts=ts, open=price, high=price * 1.01, low=price * 0.99, close=price, volume=10_000)
        )
    return bars


def test_execution_gate_long_enter() -> None:
    returns = [0.001] * 140
    close = _series(100.0, returns)
    series = _make_ohlcv(close)

    provider = StaticProvider(series=series)
    signal_map = {"AAA": {"signal": {"direction": "LONG", "confidence": 0.8}}}
    gate = ExecutionGate(data_provider=provider, signal_map=signal_map)

    outcome = gate.evaluate(["AAA"])
    payload = outcome.artifacts["AAA"]
    plan = payload["execution_plan"]

    assert outcome.decision == GateDecision.PASS
    assert plan["action"] == "ENTER"
    assert plan["side"] == "BUY"
    assert plan["stop_loss"] is not None
    assert plan["take_profit"] is not None


def test_execution_gate_short_enter() -> None:
    returns = [-0.001] * 140
    close = _series(100.0, returns)
    series = _make_ohlcv(close)

    provider = StaticProvider(series=series)
    signal_map = {"BBB": {"signal": {"direction": "SHORT", "confidence": 0.8}}}
    gate = ExecutionGate(data_provider=provider, signal_map=signal_map)

    outcome = gate.evaluate(["BBB"])
    payload = outcome.artifacts["BBB"]
    plan = payload["execution_plan"]

    assert outcome.decision == GateDecision.PASS
    assert plan["action"] == "ENTER"
    assert plan["side"] == "SELL"


def test_execution_gate_gap_risk_fails() -> None:
    returns = [0.001] * 139 + [0.0]
    close = _series(100.0, returns)
    series = _make_ohlcv(close)
    series.open[-1] = series.close[-2] * 1.2

    provider = StaticProvider(series=series)
    config = ExecutionGateConfig(gap_threshold=0.05)
    signal_map = {"CCC": {"signal": {"direction": "LONG", "confidence": 0.9}}}
    gate = ExecutionGate(data_provider=provider, config=config, signal_map=signal_map)

    outcome = gate.evaluate(["CCC"])
    payload = outcome.artifacts["CCC"]
    assert payload["quality_flags"]["gap_risk"] is True
    assert outcome.decision == GateDecision.FAIL


def test_execution_gate_missing_data() -> None:
    returns = [0.001] * 20
    close = _series(100.0, returns)
    series = _make_ohlcv(close)

    provider = StaticProvider(series=series)
    signal_map = {"DDD": {"signal": {"direction": "LONG", "confidence": 0.8}}}
    gate = ExecutionGate(data_provider=provider, signal_map=signal_map)

    outcome = gate.evaluate(["DDD"])
    payload = outcome.artifacts["DDD"]
    assert payload["quality_flags"]["missing_data"] is True
    assert outcome.decision == GateDecision.FAIL


def test_execution_gate_with_in_memory_provider() -> None:
    returns = [0.001] * 140
    close = _series(100.0, returns)

    provider = InMemoryOHLCVProvider()
    provider.set_bars("EEE", "5M", _bars_from_close(close))
    signal_map = {"EEE": {"signal": {"direction": "LONG", "confidence": 0.8}}}
    gate = ExecutionGate(ohlcv_provider=provider, signal_map=signal_map)

    outcome = gate.evaluate(["EEE"])
    payload = outcome.artifacts["EEE"]
    assert payload["execution_plan"]["action"] in {"ENTER", "HOLD"}
