from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def compute_shadow_metrics(
    trades_df: pd.DataFrame,
    equity_curve_df: pd.DataFrame,
    *,
    capital: float,
    kill_switch_triggered: bool,
) -> dict[str, Any]:
    if capital <= 0:
        raise ValueError("capital must be positive")
    if equity_curve_df.empty:
        raise ValueError("equity_curve_df must not be empty")

    equity = pd.to_numeric(equity_curve_df["equity"], errors="raise")
    total_return = float((equity.iloc[-1] / capital) - 1.0)

    returns = equity.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    if returns.empty or float(returns.std(ddof=0)) == 0.0:
        sharpe = 0.0
    else:
        sharpe = float((returns.mean() / returns.std(ddof=0)) * np.sqrt(len(returns)))

    running_max = equity.cummax()
    drawdown = (equity / running_max) - 1.0
    max_drawdown = float(drawdown.min())

    closed_trades = trades_df if not trades_df.empty else pd.DataFrame(columns=["net_pnl"])
    winners = closed_trades[closed_trades["net_pnl"] > 0.0]
    losers = closed_trades[closed_trades["net_pnl"] < 0.0]
    win_rate = float(len(winners) / len(closed_trades)) if len(closed_trades) else 0.0
    gross_profit = float(winners["net_pnl"].sum()) if len(winners) else 0.0
    gross_loss = float(abs(losers["net_pnl"].sum())) if len(losers) else 0.0
    if gross_loss == 0.0:
        profit_factor = float("inf") if gross_profit > 0.0 else 0.0
    else:
        profit_factor = float(gross_profit / gross_loss)

    return {
        "total_return": total_return,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "kill_switch_triggered": bool(kill_switch_triggered),
        "n_trades": int(len(closed_trades)),
    }


__all__ = ["compute_shadow_metrics"]
