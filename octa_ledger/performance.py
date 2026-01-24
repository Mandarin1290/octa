from __future__ import annotations

import math
from typing import List, Tuple


def periodic_returns_from_prices(prices: List[float]) -> List[float]:
    if not prices or len(prices) < 2:
        return []
    return [(prices[i + 1] / prices[i]) - 1.0 for i in range(len(prices) - 1)]


def mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def volatility(returns: List[float], sample: bool = False) -> float:
    n = len(returns)
    if n == 0:
        return 0.0
    mu = mean(returns)
    var = sum((r - mu) ** 2 for r in returns) / (n - (1 if sample and n > 1 else 0))
    return math.sqrt(var)


def annualize_return(mean_period_return: float, periods_per_year: int) -> float:
    return (1.0 + mean_period_return) ** periods_per_year - 1.0


def annualized_vol(vol_period: float, periods_per_year: int) -> float:
    return vol_period * math.sqrt(periods_per_year)


def sharpe(
    returns: List[float], risk_free: float = 0.0, periods_per_year: int = 252
) -> float:
    if not returns:
        return 0.0
    mu = mean(returns)
    vol = volatility(returns)
    if vol == 0.0:
        return float("inf") if mu - (risk_free / periods_per_year) > 0 else 0.0
    ann_excess = annualize_return(mu - (risk_free / periods_per_year), periods_per_year)
    ann_vol = annualized_vol(vol, periods_per_year)
    return ann_excess / ann_vol if ann_vol > 0 else 0.0


def downside_deviation(returns: List[float], required_return: float = 0.0) -> float:
    downs = [min(0.0, r - required_return) for r in returns]
    if not downs:
        return 0.0
    sq = sum(d * d for d in downs) / len(downs)
    return math.sqrt(sq)


def sortino(
    returns: List[float], required_return: float = 0.0, periods_per_year: int = 252
) -> float:
    if not returns:
        return 0.0
    mu = mean(returns)
    dd = downside_deviation(returns, required_return)
    if dd == 0.0:
        return float("inf") if mu - required_return > 0 else 0.0
    ann_excess = annualize_return(mu - required_return, periods_per_year)
    ann_dd = annualized_vol(dd, periods_per_year)
    return ann_excess / ann_dd if ann_dd > 0 else 0.0


def max_drawdown(prices: List[float]) -> Tuple[float, int]:
    """Return max drawdown (positive fraction) and duration (periods)."""
    peak = -float("inf")
    max_dd = 0.0
    dd_start = 0
    max_duration = 0
    for i, p in enumerate(prices):
        if p > peak:
            peak = p
            dd_start = i
        if peak <= 0:
            continue
        dd = (peak - p) / peak
        if dd > max_dd:
            max_dd = dd
            max_duration = i - dd_start
    return max_dd, max_duration


def calmar(
    returns: List[float], prices: List[float], periods_per_year: int = 252
) -> float:
    if not returns:
        return 0.0
    mu = mean(returns)
    ann_ret = annualize_return(mu, periods_per_year)
    dd, _ = max_drawdown(prices)
    if dd == 0.0:
        return float("inf") if ann_ret > 0 else 0.0
    return ann_ret / dd
