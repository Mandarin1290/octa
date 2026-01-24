from __future__ import annotations

from dataclasses import dataclass, field
from statistics import mean
from typing import Any, Mapping, Protocol, Sequence, TypedDict

from octa.core.cascade.contracts import GateDecision, GateOutcome
from octa.core.cascade.context import CascadeContext
from octa.core.data.providers.ohlcv import OHLCVBar, OHLCVProvider


@dataclass(frozen=True)
class OHLCVSeries:
    open: list[float]
    high: list[float]
    low: list[float]
    close: list[float]
    volume: list[float]
    timestamps: list[Any] = field(default_factory=list)


class ExecutionDataProvider(Protocol):
    def get_ohlcv(self, symbol: str, timeframe: str) -> OHLCVSeries | None:
        ...


class ExecutionPlan(TypedDict):
    action: str
    side: str
    stop_loss: float | None
    take_profit: float | None
    trail: float | None
    time_in_force_bars: int


@dataclass(frozen=True)
class ExecutionGateConfig:
    min_points: int = 120
    ema_fast: int = 20
    atr_window: int = 14
    momentum_window: int = 4
    gap_threshold: float = 0.08
    min_avg_volume: float = 1_000.0
    confidence_threshold: float = 0.6
    sl_atr: float = 1.5
    tp_atr: float = 2.5
    min_rr: float = 1.2


class ExecutionGate:
    name = "execution"
    timeframe = "5M"

    def __init__(
        self,
        data_provider: ExecutionDataProvider | None = None,
        config: ExecutionGateConfig | None = None,
        signal_map: Mapping[str, Mapping[str, Any]] | None = None,
        ohlcv_provider: OHLCVProvider | None = None,
    ) -> None:
        self._provider = data_provider
        self._config = config or ExecutionGateConfig()
        self._signal_map = dict(signal_map) if signal_map is not None else {}
        self._ohlcv_provider = ohlcv_provider
        self._last_artifacts: dict[str, Any] = {}

    def set_context(self, context: CascadeContext) -> None:
        signal_artifacts = context.artifacts.get("signal", {})
        resolved: dict[str, Mapping[str, Any]] = {}
        for symbol, payload in signal_artifacts.items():
            if isinstance(payload, Mapping) and symbol in payload:
                resolved[symbol] = payload[symbol]
            else:
                resolved[symbol] = payload
        self._signal_map = resolved

    def fit(self, symbols: Sequence[str]) -> None:
        return None

    def evaluate(self, symbols: Sequence[str]) -> GateOutcome:
        eligible: list[str] = []
        rejected: list[str] = []
        artifacts: dict[str, Any] = {}

        for symbol in symbols:
            payload = self._evaluate_symbol(symbol)
            artifacts[symbol] = payload
            if payload["execution_plan"]["action"] == "ENTER" and payload["decision"] == GateDecision.PASS.value:
                eligible.append(symbol)
            else:
                rejected.append(symbol)

        self._last_artifacts = artifacts
        decision = GateDecision.PASS if eligible else GateDecision.FAIL
        return GateOutcome(
            decision=decision,
            eligible_symbols=eligible,
            rejected_symbols=rejected,
            artifacts=artifacts,
        )

    def filter_universe(self, symbols: Sequence[str]) -> Sequence[str]:
        return list(symbols)

    def emit_artifacts(self, symbols: Sequence[str]) -> Mapping[str, Any]:
        return dict(self._last_artifacts)

    def _evaluate_symbol(self, symbol: str) -> dict[str, Any]:
        series = self._get_series(symbol)
        if series is None:
            return _fail_payload("missing_data")

        signal_payload = self._signal_map.get(symbol)
        if not signal_payload:
            return _fail_payload("missing_signal")

        direction = signal_payload.get("signal", {}).get("direction")
        confidence = signal_payload.get("signal", {}).get("confidence", 0.0)
        if direction not in {"LONG", "SHORT"}:
            return _fail_payload("flat_signal")

        metrics = _execution_metrics(series, self._config)
        if metrics is None:
            return _fail_payload("missing_data")

        quality_flags = _quality_flags(series, self._config)
        if quality_flags["missing_data"] or quality_flags["gap_risk"] or quality_flags["low_liquidity"]:
            return {
                "decision": GateDecision.FAIL.value,
                "execution_plan": _hold_plan(),
                "execution_metrics": metrics,
                "quality_flags": quality_flags,
            }

        plan = _build_plan(series, direction, metrics, self._config)
        if plan["action"] != "ENTER" or confidence < self._config.confidence_threshold:
            return {
                "decision": GateDecision.FAIL.value,
                "execution_plan": plan,
                "execution_metrics": metrics,
                "quality_flags": quality_flags,
            }

        return {
            "decision": GateDecision.PASS.value,
            "execution_plan": plan,
            "execution_metrics": metrics,
            "quality_flags": quality_flags,
        }

    def _get_series(self, symbol: str) -> OHLCVSeries | None:
        if self._ohlcv_provider is not None:
            bars = self._ohlcv_provider.get_ohlcv(symbol, self.timeframe)
            return _series_from_bars(bars)
        if self._provider is None:
            return None
        return self._provider.get_ohlcv(symbol, self.timeframe)


def _fail_payload(reason: str) -> dict[str, Any]:
    flags = {
        "missing_data": reason in {"missing_data", "missing_signal"},
        "gap_risk": False,
        "low_liquidity": False,
        "reason": reason,
    }
    return {
        "decision": GateDecision.FAIL.value,
        "execution_plan": _hold_plan(),
        "execution_metrics": {},
        "quality_flags": flags,
    }


def _hold_plan() -> ExecutionPlan:
    return {
        "action": "HOLD",
        "side": "NONE",
        "stop_loss": None,
        "take_profit": None,
        "trail": None,
        "time_in_force_bars": 0,
    }


def _ema(series: Sequence[float], window: int) -> list[float]:
    if not series:
        return []
    alpha = 2.0 / (window + 1)
    ema_values = [series[0]]
    for value in series[1:]:
        ema_values.append(alpha * value + (1 - alpha) * ema_values[-1])
    return ema_values


def _true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def _atr(series: OHLCVSeries, window: int) -> float | None:
    if len(series.close) < window + 1:
        return None
    ranges: list[float] = []
    for idx in range(1, len(series.close)):
        ranges.append(_true_range(series.high[idx], series.low[idx], series.close[idx - 1]))
    windowed = ranges[-window:]
    return mean(windowed) if windowed else None


def _execution_metrics(series: OHLCVSeries, config: ExecutionGateConfig) -> dict[str, float] | None:
    if len(series.close) < config.min_points:
        return None
    atr_value = _atr(series, config.atr_window)
    if atr_value is None:
        return None

    ema_fast = _ema(series.close, config.ema_fast)
    if not ema_fast:
        return None

    momentum = series.close[-1] - series.close[-config.momentum_window]
    vol5m = atr_value / series.close[-1]
    gap_score = abs(series.open[-1] / series.close[-2] - 1.0)
    slippage_risk = vol5m * 2.0

    return {
        "sl_distance_atr": config.sl_atr,
        "rr_ratio": config.tp_atr / config.sl_atr,
        "vol5m": vol5m,
        "gap_risk_score": gap_score,
        "slippage_risk_score": slippage_risk,
        "momentum": momentum,
        "ema_fast": ema_fast[-1],
    }


def _quality_flags(series: OHLCVSeries, config: ExecutionGateConfig) -> dict[str, bool]:
    missing_data = len(series.close) < config.min_points
    gap_risk = False
    if len(series.close) >= 2:
        gap_risk = abs(series.open[-1] / series.close[-2] - 1.0) >= config.gap_threshold

    avg_volume = mean(series.volume[-config.ema_fast :]) if series.volume else 0.0
    low_liquidity = avg_volume < config.min_avg_volume

    return {
        "missing_data": missing_data,
        "gap_risk": gap_risk,
        "low_liquidity": low_liquidity,
    }


def _build_plan(
    series: OHLCVSeries,
    direction: str,
    metrics: Mapping[str, float],
    config: ExecutionGateConfig,
) -> ExecutionPlan:
    close = series.close[-1]
    ema_fast = metrics["ema_fast"]
    momentum = metrics["momentum"]

    if direction == "LONG":
        if close < ema_fast or momentum <= 0:
            return _hold_plan()
        stop_loss = close - config.sl_atr * metrics["vol5m"] * close
        take_profit = close + config.tp_atr * metrics["vol5m"] * close
        rr = (take_profit - close) / max(close - stop_loss, 1e-9)
        if rr < config.min_rr:
            return _hold_plan()
        return {
            "action": "ENTER",
            "side": "BUY",
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trail": None,
            "time_in_force_bars": 6,
        }

    if direction == "SHORT":
        if close > ema_fast or momentum >= 0:
            return _hold_plan()
        stop_loss = close + config.sl_atr * metrics["vol5m"] * close
        take_profit = close - config.tp_atr * metrics["vol5m"] * close
        rr = (close - take_profit) / max(stop_loss - close, 1e-9)
        if rr < config.min_rr:
            return _hold_plan()
        return {
            "action": "ENTER",
            "side": "SELL",
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trail": None,
            "time_in_force_bars": 6,
        }

    return _hold_plan()


def _series_from_bars(bars: Sequence[OHLCVBar]) -> OHLCVSeries | None:
    if not bars:
        return None
    return OHLCVSeries(
        open=[bar.open for bar in bars],
        high=[bar.high for bar in bars],
        low=[bar.low for bar in bars],
        close=[bar.close for bar in bars],
        volume=[bar.volume for bar in bars],
        timestamps=[bar.ts for bar in bars],
    )
