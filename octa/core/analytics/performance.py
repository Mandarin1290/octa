from __future__ import annotations

from dataclasses import dataclass
from statistics import mean, pstdev
from typing import Sequence


@dataclass(frozen=True)
class PerformanceSummary:
    cumulative_return: float
    cagr: float
    max_drawdown: float
    rolling_drawdown: list[float]
    daily_returns: list[float]
    rolling_volatility: list[float]
    sharpe: float
    sortino: float
    mar: float


def compute_performance(
    equity_curve: Sequence[float],
    periods_per_year: int = 252,
    vol_window: int = 20,
) -> PerformanceSummary:
    returns = _returns_from_equity(equity_curve)
    cumulative_return = _cumulative_return(equity_curve)
    cagr = _cagr(equity_curve, periods_per_year)
    rolling_dd = rolling_drawdown(equity_curve)
    max_dd = min(rolling_dd) if rolling_dd else 0.0
    rolling_vol = rolling_volatility(returns, vol_window)
    sharpe = _sharpe(returns, periods_per_year)
    sortino = _sortino(returns, periods_per_year)
    mar = _mar(cagr, max_dd)

    return PerformanceSummary(
        cumulative_return=cumulative_return,
        cagr=cagr,
        max_drawdown=max_dd,
        rolling_drawdown=rolling_dd,
        daily_returns=returns,
        rolling_volatility=rolling_vol,
        sharpe=sharpe,
        sortino=sortino,
        mar=mar,
    )


def _returns_from_equity(equity_curve: Sequence[float]) -> list[float]:
    if len(equity_curve) < 2:
        return []
    returns: list[float] = []
    for idx in range(1, len(equity_curve)):
        prev = equity_curve[idx - 1]
        curr = equity_curve[idx]
        if prev <= 0:
            returns.append(0.0)
        else:
            returns.append(curr / prev - 1.0)
    return returns


def _cumulative_return(equity_curve: Sequence[float]) -> float:
    if len(equity_curve) < 2 or equity_curve[0] <= 0:
        return 0.0
    return equity_curve[-1] / equity_curve[0] - 1.0


def _cagr(equity_curve: Sequence[float], periods_per_year: int) -> float:
    if len(equity_curve) < 2 or equity_curve[0] <= 0:
        return 0.0
    total_periods = len(equity_curve) - 1
    years = total_periods / max(periods_per_year, 1)
    if years <= 0:
        return 0.0
    return (equity_curve[-1] / equity_curve[0]) ** (1.0 / years) - 1.0


def rolling_drawdown(equity_curve: Sequence[float]) -> list[float]:
    if not equity_curve:
        return []
    peak = equity_curve[0]
    drawdowns: list[float] = []
    for value in equity_curve:
        if value > peak:
            peak = value
        drawdowns.append(value / peak - 1.0)
    return drawdowns


def rolling_volatility(returns: Sequence[float], window: int) -> list[float]:
    if not returns:
        return []
    vols: list[float] = []
    for idx in range(len(returns)):
        window_slice = returns[max(0, idx - window + 1) : idx + 1]
        if len(window_slice) < 2:
            vols.append(0.0)
        else:
            vols.append(float(pstdev(window_slice)))
    return vols


def _sharpe(returns: Sequence[float], periods_per_year: int) -> float:
    if len(returns) < 2:
        return 0.0
    avg = mean(returns)
    vol = pstdev(returns)
    if vol == 0:
        return 0.0
    return avg / vol * (periods_per_year ** 0.5)


def _sortino(returns: Sequence[float], periods_per_year: int) -> float:
    if len(returns) < 2:
        return 0.0
    avg = mean(returns)
    downside = [ret for ret in returns if ret < 0]
    if len(downside) < 2:
        return 0.0
    downside_vol = pstdev(downside)
    if downside_vol == 0:
        return 0.0
    return avg / downside_vol * (periods_per_year ** 0.5)


def _mar(cagr: float, max_drawdown: float) -> float:
    if max_drawdown == 0:
        return 0.0
    return cagr / abs(max_drawdown)
