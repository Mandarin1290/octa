from __future__ import annotations

from typing import Any

import pandas as pd

from octa.core.paper.market_data_adapter import MarketDataAdapter, PaperMarketEvent

from .broker_paper_adapter import BrokerPaperAdapter, BrokerPaperOrder
from .broker_paper_policy import BrokerPaperPolicy


def run_broker_paper_session(
    gate_result: dict[str, Any],
    market_data_adapter: MarketDataAdapter,
    broker_adapter: BrokerPaperAdapter,
    signals_df: pd.DataFrame,
    config: BrokerPaperPolicy | dict[str, Any],
) -> dict[str, Any]:
    resolved_policy = (
        config if isinstance(config, BrokerPaperPolicy) else BrokerPaperPolicy.from_mapping(config)
    )
    if gate_result.get("status") != "BROKER_PAPER_ELIGIBLE":
        raise ValueError("broker paper session requires BROKER_PAPER_ELIGIBLE gate result")
    if broker_adapter.mode != resolved_policy.require_broker_mode:
        raise ValueError("broker adapter mode mismatch")
    if broker_adapter.mode != "PAPER":
        raise ValueError("broker adapter must remain in PAPER mode")
    if not isinstance(signals_df, pd.DataFrame) or signals_df.empty:
        raise ValueError("signals_df must be a non-empty DataFrame")
    if not isinstance(signals_df.index, pd.DatetimeIndex):
        raise TypeError("signals_df must use a DatetimeIndex")

    state: dict[str, Any] = {
        "cash": float(resolved_policy.paper_capital),
        "open_positions": {},
        "realized_pnl": 0.0,
        "peak_equity": float(resolved_policy.paper_capital),
        "kill_switch_triggered": False,
    }
    orders: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    positions: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []

    for event in market_data_adapter.iter_events():
        if event.timestamp not in signals_df.index:
            raise ValueError(f"missing signal for market event {event.timestamp.isoformat()}")
        row = signals_df.loc[event.timestamp]
        long_signal = int(row["long_signal"])
        short_signal = int(row["short_signal"])
        if long_signal and short_signal:
            raise ValueError("simultaneous long and short signal")
        target = 1 if long_signal else -1 if short_signal and resolved_policy.allow_short else 0
        symbol = event.symbol
        position = state["open_positions"].get(symbol)
        current_direction = int(position["direction"]) if position is not None else 0

        if target != current_direction:
            if position is not None:
                side = "sell" if current_direction > 0 else "buy"
                exit_order = BrokerPaperOrder(
                    timestamp=event.timestamp.isoformat(),
                    symbol=symbol,
                    side=side,
                    quantity=float(position["quantity"]),
                    reference_price=float(event.open),
                    mode="PAPER",
                )
                exit_fill = broker_adapter.submit_order(exit_order)
                orders.append(exit_order.__dict__)
                fills.append(exit_fill.__dict__)
                gross_pnl = (exit_fill.fill_price - float(position["entry_price"])) * float(position["quantity"])
                if current_direction < 0:
                    gross_pnl = (float(position["entry_price"]) - exit_fill.fill_price) * float(position["quantity"])
                state["realized_pnl"] += gross_pnl - float(position["entry_fee"]) - exit_fill.fee
                state["cash"] += (
                    float(position["quantity"]) * exit_fill.fill_price
                    if current_direction > 0
                    else -float(position["quantity"]) * exit_fill.fill_price
                ) - exit_fill.fee
                del state["open_positions"][symbol]

            if target != 0:
                if len(state["open_positions"]) >= resolved_policy.max_open_positions:
                    state["kill_switch_triggered"] = True
                    break
                allocation = float(resolved_policy.paper_capital) / float(resolved_policy.max_open_positions)
                quantity = allocation / float(event.open)
                side = "buy" if target > 0 else "sell"
                entry_order = BrokerPaperOrder(
                    timestamp=event.timestamp.isoformat(),
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    reference_price=float(event.open),
                    mode="PAPER",
                )
                entry_fill = broker_adapter.submit_order(entry_order)
                orders.append(entry_order.__dict__)
                fills.append(entry_fill.__dict__)
                if target > 0:
                    state["cash"] -= quantity * entry_fill.fill_price + entry_fill.fee
                else:
                    state["cash"] += quantity * entry_fill.fill_price - entry_fill.fee
                state["open_positions"][symbol] = {
                    "direction": target,
                    "quantity": quantity,
                    "entry_price": entry_fill.fill_price,
                    "entry_fee": entry_fill.fee,
                    "entry_time": event.timestamp,
                }

        unrealized = 0.0
        exposure = 0.0
        position = state["open_positions"].get(symbol)
        if position is not None:
            qty = float(position["quantity"])
            entry_price = float(position["entry_price"])
            if int(position["direction"]) > 0:
                exposure = qty * float(event.close)
                unrealized = (float(event.close) - entry_price) * qty
            else:
                exposure = -qty * float(event.close)
                unrealized = (entry_price - float(event.close)) * qty
        equity = float(state["cash"] + exposure)
        state["peak_equity"] = max(float(state["peak_equity"]), equity)
        drawdown = max(0.0, 1.0 - (equity / float(state["peak_equity"])))
        if drawdown > resolved_policy.kill_switch_drawdown:
            state["kill_switch_triggered"] = True
        positions.append(
            {
                "timestamp": event.timestamp,
                "symbol": symbol,
                "open_positions": int(len(state["open_positions"])),
                "cash": float(state["cash"]),
                "realized_pnl": float(state["realized_pnl"]),
                "unrealized_pnl": float(unrealized),
            }
        )
        equity_rows.append(
            {
                "timestamp": event.timestamp,
                "symbol": symbol,
                "cash": float(state["cash"]),
                "equity": float(equity),
                "drawdown": float(drawdown),
                "kill_switch": bool(state["kill_switch_triggered"]),
            }
        )
        if state["kill_switch_triggered"]:
            break

    orders_df = pd.DataFrame(orders)
    fills_df = pd.DataFrame(fills)
    positions_df = pd.DataFrame(positions).set_index("timestamp")
    equity_df = pd.DataFrame(equity_rows).set_index("timestamp")
    metrics = {
        "n_orders": int(len(orders_df)),
        "n_fills": int(len(fills_df)),
        "n_trades": int(len(fills_df) // 2),
        "final_equity": float(equity_df["equity"].iloc[-1]) if not equity_df.empty else float(resolved_policy.paper_capital),
        "max_drawdown": float(equity_df["drawdown"].max()) if not equity_df.empty else 0.0,
        "kill_switch_triggered": bool(state["kill_switch_triggered"]),
        "win_rate": float((fills_df["side"] == "sell").mean()) if not fills_df.empty else 0.0,
        "profit_factor": float("inf") if state["realized_pnl"] > 0 else 0.0,
        "total_trades": int(len(fills_df) // 2),
    }
    summary = {
        "status": "BROKER_PAPER_SESSION_ABORTED" if state["kill_switch_triggered"] else "BROKER_PAPER_SESSION_COMPLETED",
        "symbols": sorted({event.symbol for event in market_data_adapter.iter_events()}),
    }
    return {
        "session_status": summary["status"],
        "orders": orders_df,
        "fills": fills_df,
        "positions": positions_df,
        "equity_curve": equity_df,
        "metrics": metrics,
        "summary": summary,
    }


__all__ = ["run_broker_paper_session"]
