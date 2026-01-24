from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from statistics import mean, pstdev
from typing import Any, Mapping, Sequence

from octa.core.cascade.contracts import GateDecision, GateOutcome


class RegimeLabel(str, Enum):
    RISK_ON = "RISK_ON"
    RISK_OFF = "RISK_OFF"
    REDUCE = "REDUCE"
    HALT = "HALT"


@dataclass(frozen=True)
class GlobalRegimeGateConfig:
    min_points: int = 130
    return_windows: tuple[int, int, int] = (20, 60, 120)
    trend_window: int = 50
    vol_window: int = 20
    drawdown_window: int = 120
    halt_drawdown: float = 0.35
    halt_vol: float = 0.06
    risk_off_vol: float = 0.04
    reduce_vol: float = 0.03
    risk_off_trend: float = -0.02
    reduce_trend: float = 0.0
    reduce_momentum: float = 0.0


class GlobalRegimeGate:
    name = "global_regime"
    timeframe = "1D"

    def __init__(
        self,
        config: GlobalRegimeGateConfig | None = None,
        price_series: Sequence[float] | None = None,
        timestamps: Sequence[Any] | None = None,
    ) -> None:
        self._config = config or GlobalRegimeGateConfig()
        self._price_series = list(price_series) if price_series is not None else []
        self._timestamps = list(timestamps) if timestamps is not None else []
        self._last_artifacts: dict[str, Any] = {}

    def set_series(
        self, price_series: Sequence[float], timestamps: Sequence[Any] | None = None
    ) -> None:
        self._price_series = list(price_series)
        self._timestamps = list(timestamps) if timestamps is not None else []

    def fit(self, symbols: Sequence[str]) -> None:
        return None

    def evaluate(self, symbols: Sequence[str]) -> GateOutcome:
        metrics = self._compute_metrics()
        if metrics is None:
            self._last_artifacts = {
                "regime_label": RegimeLabel.HALT.value,
                "metrics": {},
                "window": self._window_metadata(),
                "status": "NO_DATA",
            }
            return GateOutcome(
                decision=GateDecision.FAIL,
                eligible_symbols=[],
                rejected_symbols=list(symbols),
                artifacts=self._last_artifacts,
            )

        regime = self._classify(metrics)
        decision = (
            GateDecision.PASS
            if regime in (RegimeLabel.RISK_ON, RegimeLabel.REDUCE)
            else GateDecision.FAIL
        )

        self._last_artifacts = {
            "regime_label": regime.value,
            "metrics": metrics,
            "window": self._window_metadata(),
        }

        eligible = list(symbols) if decision == GateDecision.PASS else []
        rejected = [] if decision == GateDecision.PASS else list(symbols)
        return GateOutcome(
            decision=decision,
            eligible_symbols=eligible,
            rejected_symbols=rejected,
            artifacts=self._last_artifacts,
        )

    def filter_universe(self, symbols: Sequence[str]) -> Sequence[str]:
        return list(symbols)

    def emit_artifacts(self, symbols: Sequence[str]) -> Mapping[str, Any]:
        return dict(self._last_artifacts)

    def _window_metadata(self) -> dict[str, Any]:
        if not self._timestamps:
            return {"points": len(self._price_series)}
        start = str(self._timestamps[0])
        end = str(self._timestamps[-1])
        return {"start": start, "end": end, "points": len(self._price_series)}

    def _compute_metrics(self) -> dict[str, float] | None:
        prices = self._price_series
        if len(prices) < self._config.min_points:
            return None
        if any(price <= 0 for price in prices):
            return None

        returns = _daily_returns(prices)
        vol = _rolling_volatility(returns, self._config.vol_window)
        drawdown = _rolling_drawdown(prices, self._config.drawdown_window)
        trend = _trend(prices, self._config.trend_window)
        momentum = _rolling_return(prices, self._config.return_windows[0])

        metrics: dict[str, float] = {
            "return_20": _rolling_return(prices, self._config.return_windows[0]),
            "return_60": _rolling_return(prices, self._config.return_windows[1]),
            "return_120": _rolling_return(prices, self._config.return_windows[2]),
            "volatility": vol,
            "drawdown": drawdown,
            "trend": trend,
            "momentum": momentum,
        }

        if any(value is None for value in metrics.values()):
            return None

        return metrics

    def _classify(self, metrics: Mapping[str, float]) -> RegimeLabel:
        drawdown = metrics["drawdown"]
        vol = metrics["volatility"]
        trend = metrics["trend"]
        momentum = metrics["momentum"]

        if drawdown <= -self._config.halt_drawdown or vol >= self._config.halt_vol:
            return RegimeLabel.HALT

        if trend <= self._config.risk_off_trend and vol >= self._config.risk_off_vol:
            return RegimeLabel.RISK_OFF

        if (
            trend <= self._config.reduce_trend
            or momentum <= self._config.reduce_momentum
            or vol >= self._config.reduce_vol
        ):
            return RegimeLabel.REDUCE

        return RegimeLabel.RISK_ON


def _daily_returns(prices: Sequence[float]) -> list[float]:
    returns: list[float] = []
    for idx in range(1, len(prices)):
        prev = prices[idx - 1]
        current = prices[idx]
        returns.append((current / prev) - 1.0)
    return returns


def _rolling_return(prices: Sequence[float], window: int) -> float | None:
    if len(prices) < window + 1:
        return None
    start = prices[-window - 1]
    end = prices[-1]
    return (end / start) - 1.0


def _rolling_volatility(returns: Sequence[float], window: int) -> float | None:
    if len(returns) < window:
        return None
    windowed = returns[-window:]
    return float(pstdev(windowed))


def _rolling_drawdown(prices: Sequence[float], window: int) -> float | None:
    if len(prices) < window:
        return None
    windowed = prices[-window:]
    peak = windowed[0]
    max_drawdown = 0.0
    for price in windowed:
        if price > peak:
            peak = price
        drawdown = (price / peak) - 1.0
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _trend(prices: Sequence[float], window: int) -> float | None:
    if len(prices) < window:
        return None
    windowed = prices[-window:]
    avg = mean(windowed)
    return (prices[-1] / avg) - 1.0
