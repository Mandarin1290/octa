from __future__ import annotations

from dataclasses import dataclass, field
from statistics import mean
from typing import Any, Mapping, Protocol, Sequence

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


class StructureDataProvider(Protocol):
    def get_ohlcv(self, symbol: str, timeframe: Timeframe) -> OHLCVSeries | None:
        ...


@dataclass(frozen=True)
class StructureZone:
    start_idx: int
    end_idx: int
    zone_type: str
    confidence: float


@dataclass(frozen=True)
class StructureGateConfig:
    min_points: int = 200
    ema_fast: int = 20
    ema_slow: int = 50
    ema_slope_lookback: int = 10
    atr_window: int = 14
    donchian_window: int = 20
    vol_stable_max: float = 0.03
    vol_high: float = 0.05
    pullback_atr_multiple: float = 1.0
    breakout_atr_multiple: float = 0.5
    gap_threshold: float = 0.08
    min_avg_volume: float = 1_000.0
    confidence_threshold: float = 0.55
    drawdown_window: int = 60
    range_window: int = 30


class StructureGate:
    name = "structure"
    timeframe: Timeframe = "30M"

    def __init__(
        self,
        data_provider: StructureDataProvider | None = None,
        config: StructureGateConfig | None = None,
        ohlcv_provider: OHLCVProvider | None = None,
    ) -> None:
        self._provider = data_provider
        self._config = config or StructureGateConfig()
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
            if payload["decision"] == GateDecision.PASS.value:
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
            return self._fail_payload(symbol, "missing_data")

        metrics = _structure_metrics(data, self._config)
        if metrics is None:
            return self._fail_payload(symbol, "missing_data")

        zones = _detect_zones(data, self._config, metrics)
        quality_flags = _quality_flags(data, self._config)

        has_valid_zone = any(zone.confidence >= self._config.confidence_threshold for zone in zones)
        quality_ok = not quality_flags["missing_data"] and not quality_flags["gap_risk"]

        decision = GateDecision.PASS if has_valid_zone and quality_ok else GateDecision.FAIL
        return {
            "decision": decision.value,
            "setup_zones": [zone.__dict__ for zone in zones],
            "structure_metrics": metrics,
            "quality_flags": quality_flags,
        }

    def _get_series(self, symbol: str) -> OHLCVSeries | None:
        if self._ohlcv_provider is not None:
            bars = self._ohlcv_provider.get_ohlcv(symbol, self.timeframe)
            return _series_from_bars(bars)
        if self._provider is None:
            return None
        return self._provider.get_ohlcv(symbol, self.timeframe)

    def _fail_payload(self, symbol: str, reason: str) -> dict[str, Any]:
        return {
            "decision": GateDecision.FAIL.value,
            "setup_zones": [],
            "structure_metrics": {},
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


def _choppiness(series: Sequence[float]) -> float | None:
    if len(series) < 2:
        return None
    total = 0.0
    for idx in range(1, len(series)):
        total += abs(series[idx] - series[idx - 1])
    net = abs(series[-1] - series[0])
    return (total / net) if net != 0 else 0.0


def _drawdown(series: Sequence[float], window: int) -> float | None:
    if len(series) < window:
        return None
    windowed = series[-window:]
    peak = windowed[0]
    max_drawdown = 0.0
    for price in windowed:
        if price > peak:
            peak = price
        drawdown = (price / peak) - 1.0
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _structure_metrics(series: OHLCVSeries, config: StructureGateConfig) -> dict[str, float] | None:
    if len(series.close) < config.min_points:
        return None
    if any(price <= 0 for price in series.close):
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
    range_score = _choppiness(series.close[-config.range_window :])
    drawdown = _drawdown(series.close, config.drawdown_window)

    if range_score is None or drawdown is None:
        return None

    vol_regime = atr_value / series.close[-1]

    return {
        "trend_score": trend_score,
        "range_score": range_score,
        "atr": atr_value,
        "vol_regime": vol_regime,
        "dd_approx": drawdown,
    }


def _quality_flags(series: OHLCVSeries, config: StructureGateConfig) -> dict[str, bool]:
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


def _detect_zones(
    series: OHLCVSeries,
    config: StructureGateConfig,
    metrics: Mapping[str, float],
) -> list[StructureZone]:
    zones: list[StructureZone] = []
    close = series.close
    _ema_fast = _ema(close, config.ema_fast)
    ema_slow = _ema(close, config.ema_slow)
    atr_value = metrics["atr"]
    vol_regime = metrics["vol_regime"]

    donchian_window = close[-config.donchian_window :]
    upper = max(donchian_window)

    trend_ok = close[-1] >= ema_slow[-1] and metrics["trend_score"] > 0
    stable_vol = vol_regime <= config.vol_stable_max

    breakout_distance = (upper - close[-1])
    if trend_ok and stable_vol and breakout_distance <= config.breakout_atr_multiple * atr_value:
        confidence = min(1.0, 0.6 + (config.vol_stable_max - vol_regime))
        zones.append(
            StructureZone(
                start_idx=len(close) - config.donchian_window,
                end_idx=len(close) - 1,
                zone_type="breakout",
                confidence=confidence,
            )
        )

    pullback_distance = abs(close[-1] - _ema_fast[-1])
    if trend_ok and stable_vol and pullback_distance <= config.pullback_atr_multiple * atr_value:
        confidence = min(1.0, 0.5 + (config.vol_stable_max - vol_regime))
        zones.append(
            StructureZone(
                start_idx=len(close) - config.ema_fast,
                end_idx=len(close) - 1,
                zone_type="pullback",
                confidence=confidence,
            )
        )

    if not zones and vol_regime >= config.vol_high and metrics["range_score"] > 1.5:
        confidence = 0.4
        zones.append(
            StructureZone(
                start_idx=len(close) - config.range_window,
                end_idx=len(close) - 1,
                zone_type="range",
                confidence=confidence,
            )
        )

    return zones


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
