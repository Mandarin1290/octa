from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from octa.core.cascade.context import CascadeContext
from octa.core.cascade.contracts import GateDecision
from octa.core.data.providers.in_memory import InMemoryOHLCVProvider
from octa.core.data.providers.ohlcv import OHLCVBar
from octa.core.gates.micro_optimization.gate import (
    MicroDataProvider,
    MicroGate,
    MicroGateConfig,
    OHLCVSeries,
)


@dataclass
class StaticProvider(MicroDataProvider):
    series: OHLCVSeries

    def get_ohlcv(self, symbol: str, timeframe: str) -> OHLCVSeries | None:
        return self.series


def _series(start: float, returns: list[float]) -> list[float]:
    prices = [start]
    for ret in returns:
        prices.append(prices[-1] * (1.0 + ret))
    return prices


def _make_ohlcv(close: list[float], vol: float = 10_000.0) -> OHLCVSeries:
    high = [price * 1.001 for price in close]
    low = [price * 0.999 for price in close]
    open_prices = close[:-1] + [close[-1]]
    volume = [vol for _ in close]
    return OHLCVSeries(open=open_prices, high=high, low=low, close=close, volume=volume)


def _bars_from_close(close: list[float]) -> list[OHLCVBar]:
    start = datetime(2024, 1, 1)
    bars: list[OHLCVBar] = []
    for idx, price in enumerate(close):
        ts = start + timedelta(minutes=1 * idx)
        bars.append(
            OHLCVBar(ts=ts, open=price, high=price * 1.001, low=price * 0.999, close=price, volume=10_000)
        )
    return bars


def _context_with_enter(symbol: str) -> CascadeContext:
    ctx = CascadeContext()
    ctx.artifacts["execution"] = {
        symbol: {
            "execution_plan": {
                "action": "ENTER",
                "side": "BUY",
                "stop_loss": 95.0,
                "take_profit": 110.0,
                "trail": None,
                "time_in_force_bars": 6,
            }
        }
    }
    return ctx


def test_micro_gate_limit_plan_on_calm_series() -> None:
    returns = [0.0005] * 140
    close = _series(100.0, returns)
    series = _make_ohlcv(close)

    provider = StaticProvider(series=series)
    gate = MicroGate(data_provider=provider)
    gate.set_context(_context_with_enter("AAA"))

    outcome = gate.evaluate(["AAA"])
    payload = outcome.artifacts["AAA"]
    plan = payload["micro_plan"]

    assert outcome.decision == GateDecision.PASS
    assert plan["order_type_hint"] == "LIMIT"
    assert plan["limit_offset_bps"] >= gate._config.min_offset_bps


def test_micro_gate_fails_on_spike_risk() -> None:
    returns = [0.0] * 135 + [0.02] + [0.0] * 4
    close = _series(100.0, returns)
    series = _make_ohlcv(close)

    provider = StaticProvider(series=series)
    config = MicroGateConfig(spike_score_high=0.005)
    gate = MicroGate(data_provider=provider, config=config)
    gate.set_context(_context_with_enter("BBB"))

    outcome = gate.evaluate(["BBB"])
    payload = outcome.artifacts["BBB"]
    assert payload["micro_risk_flags"]["spike_risk"] is True
    assert outcome.decision == GateDecision.FAIL


def test_micro_gate_missing_execution_plan_fails() -> None:
    returns = [0.0005] * 140
    close = _series(100.0, returns)
    series = _make_ohlcv(close)

    provider = StaticProvider(series=series)
    gate = MicroGate(data_provider=provider)

    outcome = gate.evaluate(["CCC"])
    payload = outcome.artifacts["CCC"]
    assert payload["micro_risk_flags"]["reason"] == "missing_execution_plan"
    assert outcome.decision == GateDecision.FAIL


def test_micro_gate_missing_data_fails() -> None:
    returns = [0.0005] * 20
    close = _series(100.0, returns)
    series = _make_ohlcv(close)

    provider = StaticProvider(series=series)
    gate = MicroGate(data_provider=provider)
    gate.set_context(_context_with_enter("DDD"))

    outcome = gate.evaluate(["DDD"])
    payload = outcome.artifacts["DDD"]
    assert payload["micro_risk_flags"]["reason"] == "missing_data"
    assert outcome.decision == GateDecision.FAIL


def test_micro_gate_with_in_memory_provider() -> None:
    returns = [0.0004] * 140
    close = _series(100.0, returns)

    provider = InMemoryOHLCVProvider()
    provider.set_bars("EEE", "1M", _bars_from_close(close))
    gate = MicroGate(ohlcv_provider=provider)
    gate.set_context(_context_with_enter("EEE"))

    outcome = gate.evaluate(["EEE"])
    payload = outcome.artifacts["EEE"]
    assert payload["micro_plan"]["order_type_hint"] in {"LIMIT", "MARKET"}
