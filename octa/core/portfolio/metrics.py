from __future__ import annotations

from statistics import mean, pstdev
from typing import Mapping, Sequence


def portfolio_volatility(returns: Sequence[float], window: int) -> float:
    if len(returns) < window:
        return 0.0
    return float(pstdev(returns[-window:]))


def rolling_drawdown(equity_curve: Sequence[float], window: int) -> float:
    if len(equity_curve) < window:
        return 0.0
    windowed = equity_curve[-window:]
    peak = windowed[0]
    max_drawdown = 0.0
    for value in windowed:
        if value > peak:
            peak = value
        drawdown = (value / peak) - 1.0
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def trade_density(trade_counts: Sequence[int], window: int) -> float:
    if len(trade_counts) < window:
        return 0.0
    return mean(trade_counts[-window:])


def exposure_concentration(positions: Mapping[str, float]) -> float:
    if not positions:
        return 0.0
    total = sum(abs(value) for value in positions.values())
    if total == 0:
        return 0.0
    weights = [abs(value) / total for value in positions.values()]
    return sum(weight ** 2 for weight in weights)


def turnover_rate(positions: Mapping[str, float], prev_positions: Mapping[str, float]) -> float:
    if not positions and not prev_positions:
        return 0.0
    total = sum(abs(value) for value in positions.values()) + sum(
        abs(value) for value in prev_positions.values()
    )
    if total == 0:
        return 0.0
    turnover = sum(abs(positions.get(symbol, 0.0) - prev_positions.get(symbol, 0.0)) for symbol in set(positions) | set(prev_positions))
    return turnover / total
