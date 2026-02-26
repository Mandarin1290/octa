from __future__ import annotations

import math
from dataclasses import dataclass, field
from statistics import mean
from typing import Any, Mapping, Protocol, Sequence, TypedDict

from octa.core.cascade.contracts import GateDecision, GateOutcome
from octa.core.data.providers.ohlcv import OHLCVBar, OHLCVProvider
from octa.core.types.timeframe import Timeframe


@dataclass(frozen=True)
class OHLCVSeries:
    open: list[float]
    high: list[float]
    low: list[float]
    close: list[float]
    volume: list[float]
    timestamps: list[Any] = field(default_factory=list)


class SignalDataProvider(Protocol):
    def get_ohlcv(self, symbol: str, timeframe: Timeframe) -> OHLCVSeries | None:
        ...


class SignalQualityFlags(TypedDict):
    missing_data: bool
    gap_risk: bool
    low_liquidity: bool


class SignalPayload(TypedDict):
    signal: dict[str, Any]
    signal_metrics: dict[str, float]
    quality_flags: SignalQualityFlags


@dataclass(frozen=True)
class SignalGateConfig:
    min_points: int = 200
    ema_fast: int = 20
    ema_slow: int = 50
    ema_slope_lookback: int = 10
    atr_window: int = 14
    donchian_window: int = 20
    momentum_short: int = 8
    momentum_long: int = 16
    gap_threshold: float = 0.08
    min_avg_volume: float = 1_000.0
    confidence_threshold: float = 0.6
    score_margin: float = 0.1
    confidence_scale: float = 0.8
    low_vol_cutoff: float = 0.02
    high_vol_cutoff: float = 0.05
    reversal_risk_max: float = 3.0


class SignalGate:
    name = "signal"
    timeframe: Timeframe = "1H"

    def __init__(
        self,
        data_provider: SignalDataProvider | None = None,
        config: SignalGateConfig | None = None,
        ohlcv_provider: OHLCVProvider | None = None,
    ) -> None:
        self._provider = data_provider
        self._config = config or SignalGateConfig()
        self._ohlcv_provider = ohlcv_provider
        self._last_artifacts: dict[str, Any] = {}

    def fit(self, symbols: Sequence[str]) -> None:
        return None

    def evaluate(self, symbols: Sequence[str]) -> GateOutcome:
        eligible: list[str] = []
        rejected: list[str] = []
        artifacts: dict[str, Any] = {}

        for symbol in symbols:
            payload = self._evaluate_symbol(symbol)
            artifacts[symbol] = payload
            if payload["signal"]["direction"] != "FLAT" and payload["decision"] == GateDecision.PASS.value:
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
        data = self._get_series(symbol)
        if data is None:
            return _fail_payload("missing_data")

        metrics = _signal_metrics(data, self._config)
        if metrics is None:
            return _fail_payload("missing_data")

        quality_flags = _quality_flags(data, self._config)
        direction, confidence, horizon = _direction_and_confidence(metrics, self._config)
        decision = _decision(direction, confidence, quality_flags, self._config)

        return {
            "decision": decision.value,
            "signal": {
                "direction": direction,
                "confidence": confidence,
                "horizon_bars": horizon,
            },
            "signal_metrics": metrics,
            "quality_flags": quality_flags,
        }

    def _get_series(self, symbol: str) -> OHLCVSeries | None:
        if self._ohlcv_provider is not None:
            bars = self._ohlcv_provider.get_ohlcv(symbol, self.timeframe)
            return _series_from_bars(bars)
        if self._provider is None:
            return None
        return self._provider.get_ohlcv(symbol, self.timeframe)


def _decision(
    direction: str,
    confidence: float,
    flags: SignalQualityFlags,
    config: SignalGateConfig,
) -> GateDecision:
    if flags["missing_data"] or flags["gap_risk"] or flags["low_liquidity"]:
        return GateDecision.FAIL
    if direction == "FLAT":
        return GateDecision.FAIL
    if confidence < config.confidence_threshold:
        return GateDecision.FAIL
    return GateDecision.PASS


def _fail_payload(reason: str) -> dict[str, Any]:
    return {
        "decision": GateDecision.FAIL.value,
        "signal": {"direction": "FLAT", "confidence": 0.0, "horizon_bars": 0},
        "signal_metrics": {},
        "quality_flags": {
            "missing_data": True,
            "gap_risk": False,
            "low_liquidity": False,
            "reason": reason,
        },
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


def _rolling_return(series: Sequence[float], window: int) -> float | None:
    if len(series) < window + 1:
        return None
    start = series[-window - 1]
    end = series[-1]
    return (end / start) - 1.0


def _choppiness(series: Sequence[float]) -> float | None:
    if len(series) < 2:
        return None
    total = 0.0
    for idx in range(1, len(series)):
        total += abs(series[idx] - series[idx - 1])
    net = abs(series[-1] - series[0])
    if net == 0:
        return float("inf")
    return total / net


def _signal_metrics(series: OHLCVSeries, config: SignalGateConfig) -> dict[str, float] | None:
    if len(series.close) < config.min_points:
        return None
    if any((not math.isfinite(price)) or price <= 0 for price in series.close):
        return None

    _ema_fast = _ema(series.close, config.ema_fast)
    ema_slow = _ema(series.close, config.ema_slow)
    if len(ema_slow) < config.ema_slope_lookback + 1:
        return None

    atr_value = _atr(series, config.atr_window)
    if atr_value is None:
        return None

    slope_window = ema_slow[-config.ema_slope_lookback - 1 :]
    slope = (slope_window[-1] / slope_window[0]) - 1.0
    trend_score = (series.close[-1] / ema_slow[-1]) - 1.0 + slope

    momentum_short = _rolling_return(series.close, config.momentum_short)
    momentum_long = _rolling_return(series.close, config.momentum_long)
    if momentum_short is None or momentum_long is None:
        return None
    momentum_score = 0.6 * momentum_short + 0.4 * momentum_long

    vol_regime = atr_value / series.close[-1]
    volatility_score = -vol_regime

    donchian = series.close[-config.donchian_window :]
    upper = max(donchian)
    lower = min(donchian)
    breakout_score = 0.0
    if upper > 0 and lower > 0:
        breakout_score = (series.close[-1] - lower) / (upper - lower + 1e-9)

    choppy = _choppiness(series.close[-config.donchian_window :])
    reversal_risk = choppy if choppy is not None else 0.0

    return {
        "momentum_score": momentum_score,
        "trend_score": trend_score,
        "volatility_score": volatility_score,
        "breakout_score": breakout_score,
        "reversal_risk": reversal_risk,
        "vol_regime": vol_regime,
    }


def _quality_flags(series: OHLCVSeries, config: SignalGateConfig) -> SignalQualityFlags:
    missing_data = len(series.close) < config.min_points
    gap_risk = False
    for idx in range(1, len(series.close)):
        ret = (series.close[idx] / series.close[idx - 1]) - 1.0
        if abs(ret) >= config.gap_threshold:
            gap_risk = True
            break

    avg_volume = mean(series.volume[-config.ema_slow :]) if series.volume else 0.0
    low_liquidity = avg_volume < config.min_avg_volume

    return {
        "missing_data": missing_data,
        "gap_risk": gap_risk,
        "low_liquidity": low_liquidity,
    }


def _direction_and_confidence(
    metrics: Mapping[str, float], config: SignalGateConfig
) -> tuple[str, float, int]:
    momentum = metrics["momentum_score"]
    trend = metrics["trend_score"]
    breakout = metrics["breakout_score"]
    reversal_risk = metrics["reversal_risk"]
    vol_regime = metrics["vol_regime"]

    if reversal_risk >= config.reversal_risk_max:
        direction = "FLAT"
        confidence = 0.0
        horizon = 12
        if vol_regime <= config.low_vol_cutoff:
            horizon = 24
        elif vol_regime >= config.high_vol_cutoff:
            horizon = 6
        return direction, confidence, horizon

    long_score = momentum + trend + breakout - reversal_risk
    short_score = -momentum - trend + (1.0 - breakout) - reversal_risk

    score_gap = long_score - short_score
    if score_gap >= config.score_margin:
        direction = "LONG"
    elif score_gap <= -config.score_margin:
        direction = "SHORT"
    else:
        direction = "FLAT"

    confidence = min(1.0, max(0.0, abs(score_gap) / config.confidence_scale))

    if vol_regime <= config.low_vol_cutoff:
        horizon = 24
    elif vol_regime >= config.high_vol_cutoff:
        horizon = 6
    else:
        horizon = 12

    return direction, confidence, horizon


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
