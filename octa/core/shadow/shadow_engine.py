"""RESEARCH SHADOW ENGINE.

This module provides run_shadow_trading() — a vector backtest that replays historical
signals on historical prices. It requires pre-computed signals_df and prices_df as input.

Layer: RESEARCH ONLY. No broker connection. No governance audit chain. No order emission.

For the production shadow (dry-run with real broker read-only and full governance),
see octa/execution/runner.py::run_execution(mode="dry-run").
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from .execution_rules import (
    apply_slippage,
    build_trade_record,
    coerce_price_frame,
    target_positions_from_signals,
)
from .metrics import compute_shadow_metrics
from .risk_overlay import enforce_risk_overlay


REQUIRED_CONFIG_KEYS = ("position_size", "fee", "slippage", "capital")


def _validate_config(config: dict[str, Any]) -> dict[str, Any]:
    missing = [key for key in REQUIRED_CONFIG_KEYS if key not in config]
    if missing:
        raise ValueError(f"config missing required keys: {missing}")
    normalized = dict(config)
    for key in REQUIRED_CONFIG_KEYS:
        normalized[key] = float(normalized[key])
    if normalized["position_size"] <= 0:
        raise ValueError("position_size must be positive")
    if normalized["fee"] < 0:
        raise ValueError("fee must be non-negative")
    if normalized["slippage"] < 0:
        raise ValueError("slippage must be non-negative")
    if normalized["capital"] <= 0:
        raise ValueError("capital must be positive")
    normalized["allow_short"] = bool(normalized.get("allow_short", True))
    normalized["max_drawdown_limit"] = float(normalized.get("max_drawdown_limit", 1.0))
    normalized["max_position_size"] = float(normalized.get("max_position_size", 1.0))
    normalized["max_equity_jump"] = float(normalized.get("max_equity_jump", 1.0))
    return normalized


def run_shadow_trading(
    signals_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    config: dict[str, Any],
) -> dict[str, Any]:
    normalized_config = _validate_config(config)
    price_frame = coerce_price_frame(prices_df)

    if not isinstance(signals_df, pd.DataFrame):
        raise TypeError("signals_df must be a pandas DataFrame")
    if not isinstance(signals_df.index, pd.DatetimeIndex):
        raise TypeError("signals_df must use a DatetimeIndex")
    if not signals_df.index.is_monotonic_increasing:
        raise ValueError("signals_df index must be monotonic increasing")

    common_index = signals_df.index.intersection(price_frame.index)
    if len(common_index) < 2:
        raise ValueError("signals_df and prices_df must share at least two timestamps")

    aligned_signals = signals_df.loc[common_index]
    aligned_prices = price_frame.loc[common_index]
    target_positions = target_positions_from_signals(
        aligned_signals,
        allow_short=normalized_config["allow_short"],
    )

    capital = normalized_config["capital"]
    fee_rate = normalized_config["fee"]
    slippage = normalized_config["slippage"]
    target_notional = capital * normalized_config["position_size"]

    current_quantity = 0.0
    current_direction = 0
    cash = capital
    peak_equity = capital
    kill_switch_triggered = False
    pending_target = int(target_positions.iloc[0])
    trade_id = 0
    open_trade: dict[str, Any] | None = None
    trade_rows: list[dict[str, Any]] = []
    equity_rows = [
        {
            "timestamp": common_index[0],
            "cash": float(cash),
            "equity": float(cash),
            "position": int(current_direction),
            "quantity": float(current_quantity),
            "drawdown": 0.0,
            "kill_switch": False,
        }
    ]

    for offset in range(1, len(common_index)):
        ts = common_index[offset]
        open_price = float(aligned_prices.iloc[offset]["open"])
        close_price = float(aligned_prices.iloc[offset]["close"])

        if pending_target != current_direction:
            if current_direction != 0:
                exit_side = "sell" if current_direction > 0 else "buy"
                exit_fill = apply_slippage(open_price, exit_side, slippage)
                exit_fee = abs(current_quantity * exit_fill) * fee_rate
                cash -= (-current_quantity) * exit_fill + exit_fee
                if open_trade is None:
                    raise RuntimeError("missing open trade state for exit")
                trade_id += 1
                trade_rows.append(
                    build_trade_record(
                        trade_id=trade_id,
                        direction=open_trade["direction"],
                        entry_time=open_trade["entry_time"],
                        entry_price=open_trade["entry_price"],
                        exit_time=ts,
                        exit_price=exit_fill,
                        quantity=abs(open_trade["quantity"]),
                        entry_fee=open_trade["entry_fee"],
                        exit_fee=exit_fee,
                    )
                )
                open_trade = None
                current_quantity = 0.0
                current_direction = 0

            if pending_target != 0 and not kill_switch_triggered:
                entry_side = "buy" if pending_target > 0 else "sell"
                entry_fill = apply_slippage(open_price, entry_side, slippage)
                quantity = target_notional / entry_fill
                signed_quantity = quantity if pending_target > 0 else -quantity
                entry_fee = abs(signed_quantity * entry_fill) * fee_rate
                cash -= signed_quantity * entry_fill + entry_fee
                current_quantity = signed_quantity
                current_direction = pending_target
                open_trade = {
                    "direction": "long" if pending_target > 0 else "short",
                    "entry_time": ts,
                    "entry_price": entry_fill,
                    "entry_fee": entry_fee,
                    "quantity": abs(quantity),
                }

        equity = float(cash + (current_quantity * close_price))
        peak_equity = max(peak_equity, equity)
        risk_state = enforce_risk_overlay(
            config=normalized_config,
            current_equity=equity,
            peak_equity=peak_equity,
        )
        kill_switch_triggered = kill_switch_triggered or bool(risk_state["kill_switch_triggered"])
        equity_rows.append(
            {
                "timestamp": ts,
                "cash": float(cash),
                "equity": float(equity),
                "position": int(current_direction),
                "quantity": float(current_quantity),
                "drawdown": float(risk_state["drawdown"]),
                "kill_switch": bool(kill_switch_triggered),
            }
        )
        if kill_switch_triggered:
            pending_target = 0
        else:
            pending_target = int(target_positions.iloc[offset])

    trades_df = pd.DataFrame(trade_rows)
    if not trades_df.empty:
        trades_df = trades_df.set_index("trade_id")

    equity_curve_df = pd.DataFrame(equity_rows).set_index("timestamp")
    metrics = compute_shadow_metrics(
        trades_df,
        equity_curve_df,
        capital=capital,
        kill_switch_triggered=kill_switch_triggered,
    )
    return {
        "trades": trades_df,
        "equity_curve": equity_curve_df,
        "metrics": metrics,
    }


__all__ = ["run_shadow_trading"]
