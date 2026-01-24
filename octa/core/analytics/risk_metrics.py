from __future__ import annotations

from dataclasses import dataclass
from statistics import mean, pstdev
from typing import Sequence


@dataclass(frozen=True)
class RiskSummary:
    var: float
    cvar: float
    volatility_regime: str
    drawdown_duration: int
    tail_risk_proxy: float


def compute_risk_metrics(returns: Sequence[float], drawdowns: Sequence[float]) -> RiskSummary:
    var_value = _var(returns)
    cvar_value = _cvar(returns)
    volatility_regime = _vol_regime(returns)
    drawdown_duration = _drawdown_duration(drawdowns)
    tail_risk_proxy = _tail_risk_proxy(returns)

    return RiskSummary(
        var=var_value,
        cvar=cvar_value,
        volatility_regime=volatility_regime,
        drawdown_duration=drawdown_duration,
        tail_risk_proxy=tail_risk_proxy,
    )


def _var(returns: Sequence[float], percentile: float = 0.05) -> float:
    if not returns:
        return 0.0
    sorted_returns = sorted(returns)
    idx = int(len(sorted_returns) * percentile)
    return sorted_returns[min(idx, len(sorted_returns) - 1)]


def _cvar(returns: Sequence[float], percentile: float = 0.05) -> float:
    if not returns:
        return 0.0
    sorted_returns = sorted(returns)
    idx = int(len(sorted_returns) * percentile)
    tail = sorted_returns[: max(idx, 1)]
    return mean(tail) if tail else 0.0


def _vol_regime(returns: Sequence[float]) -> str:
    if len(returns) < 2:
        return "LOW"
    vol = pstdev(returns)
    if vol >= 0.05:
        return "HIGH"
    if vol >= 0.02:
        return "MEDIUM"
    return "LOW"


def _drawdown_duration(drawdowns: Sequence[float]) -> int:
    max_duration = 0
    current = 0
    for value in drawdowns:
        if value < 0:
            current += 1
            max_duration = max(max_duration, current)
        else:
            current = 0
    return max_duration


def _tail_risk_proxy(returns: Sequence[float]) -> float:
    if len(returns) < 2:
        return 0.0
    vol = pstdev(returns)
    if vol == 0:
        return 0.0
    threshold = -2.0 * vol
    tail = [ret for ret in returns if ret <= threshold]
    return len(tail) / len(returns)
