from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def validate_shadow_run(
    trades_df: pd.DataFrame,
    equity_curve_df: pd.DataFrame,
    *,
    max_equity_jump: float = 1.0,
) -> dict[str, Any]:
    if not isinstance(trades_df, pd.DataFrame):
        raise TypeError("trades_df must be a pandas DataFrame")
    if not isinstance(equity_curve_df, pd.DataFrame):
        raise TypeError("equity_curve_df must be a pandas DataFrame")
    if equity_curve_df.empty:
        raise ValueError("equity_curve_df must not be empty")
    if not isinstance(equity_curve_df.index, pd.DatetimeIndex):
        raise TypeError("equity_curve_df must use a DatetimeIndex")
    if not equity_curve_df.index.is_monotonic_increasing:
        raise ValueError("equity_curve_df index must be monotonic increasing")
    if equity_curve_df.isna().any().any():
        raise ValueError("equity_curve_df contains NaNs")

    equity = pd.to_numeric(equity_curve_df["equity"], errors="raise")
    if not np.isfinite(equity.to_numpy(dtype=float)).all():
        raise ValueError("equity_curve_df contains non-finite equity values")

    jumps = equity.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    if not jumps.empty and float(np.abs(jumps).max()) > max_equity_jump:
        raise ValueError("equity_curve_df contains unrealistic jumps")

    if not trades_df.empty:
        if trades_df.isna().any().any():
            raise ValueError("trades_df contains NaNs")
        required = {
            "direction",
            "entry_time",
            "exit_time",
            "entry_price",
            "exit_price",
            "quantity",
            "net_pnl",
        }
        missing = sorted(required - set(trades_df.columns))
        if missing:
            raise ValueError(f"trades_df missing required columns: {missing}")
        if not trades_df["direction"].isin(["long", "short"]).all():
            raise ValueError("trades_df contains unsupported directions")
        if (pd.to_numeric(trades_df["quantity"], errors="raise") <= 0).any():
            raise ValueError("trades_df contains non-positive quantities")
        entry_times = pd.to_datetime(trades_df["entry_time"], utc=True)
        exit_times = pd.to_datetime(trades_df["exit_time"], utc=True)
        if (exit_times < entry_times).any():
            raise ValueError("trades_df contains exits before entries")
        if not entry_times.is_monotonic_increasing:
            raise ValueError("trades_df entry_time must be monotonic increasing")

    return {
        "status": "ok",
        "n_trades": int(len(trades_df)),
        "n_equity_rows": int(len(equity_curve_df)),
        "max_equity_jump": float(np.abs(jumps).max()) if not jumps.empty else 0.0,
    }


__all__ = ["validate_shadow_run"]
