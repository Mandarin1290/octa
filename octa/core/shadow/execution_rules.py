from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


REQUIRED_SIGNAL_COLUMNS = ("long_signal", "short_signal")


def coerce_price_frame(prices_df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(prices_df, pd.DataFrame):
        raise TypeError("prices_df must be a pandas DataFrame")
    if prices_df.empty:
        raise ValueError("prices_df must not be empty")
    if not isinstance(prices_df.index, pd.DatetimeIndex):
        raise TypeError("prices_df must use a DatetimeIndex")
    if not prices_df.index.is_monotonic_increasing:
        raise ValueError("prices_df index must be monotonic increasing")

    lowered = {str(column).lower(): column for column in prices_df.columns}
    open_column = lowered.get("open")
    close_column = lowered.get("close")
    if open_column is None and close_column is None:
        if len(prices_df.columns) == 1:
            only_column = prices_df.columns[0]
            open_column = only_column
            close_column = only_column
        else:
            raise ValueError("prices_df must contain at least 'open' or 'close'")
    if open_column is None:
        open_column = close_column
    if close_column is None:
        close_column = open_column

    frame = pd.DataFrame(
        {
            "open": pd.to_numeric(prices_df[open_column], errors="raise"),
            "close": pd.to_numeric(prices_df[close_column], errors="raise"),
        },
        index=prices_df.index,
    )
    if (frame <= 0).any().any():
        raise ValueError("prices_df contains non-positive prices")
    return frame


def target_positions_from_signals(
    signals_df: pd.DataFrame,
    *,
    allow_short: bool,
) -> pd.Series:
    if not isinstance(signals_df, pd.DataFrame):
        raise TypeError("signals_df must be a pandas DataFrame")
    if signals_df.empty:
        raise ValueError("signals_df must not be empty")
    missing = [name for name in REQUIRED_SIGNAL_COLUMNS if name not in signals_df.columns]
    if missing:
        raise ValueError(f"signals_df missing required columns: {missing}")

    long_signal = pd.to_numeric(signals_df["long_signal"], errors="raise").fillna(0.0)
    short_signal = pd.to_numeric(signals_df["short_signal"], errors="raise").fillna(0.0)
    if ((long_signal > 0) & (short_signal > 0)).any():
        raise ValueError("signals_df contains simultaneous long and short signals")

    target = pd.Series(0, index=signals_df.index, dtype=int)
    target.loc[long_signal > 0] = 1
    if allow_short:
        target.loc[short_signal > 0] = -1
    return target


def apply_slippage(price: float, side: str, slippage: float) -> float:
    if side not in {"buy", "sell"}:
        raise ValueError(f"unsupported execution side: {side}")
    if slippage < 0:
        raise ValueError("slippage must be non-negative")
    multiplier = 1.0 + slippage if side == "buy" else 1.0 - slippage
    adjusted = price * multiplier
    if adjusted <= 0:
        raise ValueError("slippage-adjusted price must remain positive")
    return adjusted


def build_trade_record(
    *,
    trade_id: int,
    direction: str,
    entry_time: pd.Timestamp,
    entry_price: float,
    exit_time: pd.Timestamp,
    exit_price: float,
    quantity: float,
    entry_fee: float,
    exit_fee: float,
) -> dict[str, Any]:
    gross_pnl = (exit_price - entry_price) * quantity
    if direction == "short":
        gross_pnl = (entry_price - exit_price) * quantity
    net_pnl = gross_pnl - entry_fee - exit_fee
    return {
        "trade_id": trade_id,
        "direction": direction,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "entry_price": float(entry_price),
        "exit_price": float(exit_price),
        "quantity": float(quantity),
        "entry_fee": float(entry_fee),
        "exit_fee": float(exit_fee),
        "gross_pnl": float(gross_pnl),
        "net_pnl": float(net_pnl),
    }


__all__ = [
    "REQUIRED_SIGNAL_COLUMNS",
    "apply_slippage",
    "build_trade_record",
    "coerce_price_frame",
    "target_positions_from_signals",
]
