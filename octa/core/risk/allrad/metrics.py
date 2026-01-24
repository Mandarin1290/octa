from __future__ import annotations

from dataclasses import dataclass
from statistics import mean, pstdev
from typing import Sequence


@dataclass(frozen=True)
class RiskMetrics:
    rolling_volatility: float
    rolling_drawdown: float
    loss_streak: int
    capital_exposure: float
    slippage_proxy: float
    trade_density: float


def rolling_volatility(returns: Sequence[float], window: int) -> float:
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


def loss_streak(pnl_series: Sequence[float]) -> int:
    streak = 0
    for pnl in reversed(pnl_series):
        if pnl < 0:
            streak += 1
        else:
            break
    return streak


def capital_exposure(position_values: Sequence[float], capital_base: float) -> float:
    if capital_base <= 0:
        return 0.0
    return sum(position_values) / capital_base


def slippage_proxy(prices: Sequence[float]) -> float:
    if len(prices) < 2:
        return 0.0
    diffs = [abs(prices[idx] - prices[idx - 1]) for idx in range(1, len(prices))]
    return mean(diffs) if diffs else 0.0


def trade_density(trade_counts: Sequence[int], window: int) -> float:
    if len(trade_counts) < window:
        return 0.0
    return mean(trade_counts[-window:])
