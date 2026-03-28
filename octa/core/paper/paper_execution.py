from __future__ import annotations

from typing import Any

from .market_data_adapter import PaperMarketEvent


def _apply_slippage(price: float, side: str, slippage: float) -> float:
    if side not in {"buy", "sell"}:
        raise ValueError(f"unsupported side: {side}")
    if slippage < 0:
        raise ValueError("paper_slippage must be non-negative")
    adjusted = price * (1.0 + slippage if side == "buy" else 1.0 - slippage)
    if adjusted <= 0:
        raise ValueError("adjusted execution price must be positive")
    return adjusted


def simulate_paper_order(
    signal: int,
    market_event: PaperMarketEvent,
    state: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    if signal not in {-1, 0, 1}:
        raise ValueError(f"unsupported signal: {signal}")
    if signal == -1 and not bool(config["allow_short"]):
        signal = 0

    symbol = market_event.symbol
    positions = state["open_positions"]
    current = positions.get(symbol)
    current_direction = int(current["direction"]) if current is not None else 0
    fee_rate = float(config["paper_fee"])
    slippage = float(config["paper_slippage"])
    max_open_positions = int(config["max_open_positions"])
    if max_open_positions <= 0:
        raise ValueError("max_open_positions must be positive")
    allocation = float(config["paper_capital"]) / float(max_open_positions)

    if current_direction == signal:
        return state

    if current is not None:
        exit_side = "sell" if current_direction > 0 else "buy"
        exit_price = _apply_slippage(float(market_event.open), exit_side, slippage)
        quantity = float(current["quantity"])
        exit_fee = abs(quantity * exit_price) * fee_rate
        gross_pnl = (exit_price - float(current["entry_price"])) * quantity
        if current_direction < 0:
            gross_pnl = (float(current["entry_price"]) - exit_price) * quantity
        net_pnl = gross_pnl - float(current["entry_fee"]) - exit_fee
        state["cash"] += (quantity * exit_price if current_direction > 0 else -quantity * exit_price) - exit_fee
        state["realized_pnl"] += net_pnl
        state["trade_log"].append(
            {
                "symbol": symbol,
                "direction": "long" if current_direction > 0 else "short",
                "entry_time": current["entry_time"],
                "exit_time": market_event.timestamp,
                "entry_price": float(current["entry_price"]),
                "exit_price": float(exit_price),
                "quantity": float(quantity),
                "entry_fee": float(current["entry_fee"]),
                "exit_fee": float(exit_fee),
                "net_pnl": float(net_pnl),
            }
        )
        del positions[symbol]

    if signal == 0:
        return state

    if len(positions) >= max_open_positions:
        raise ValueError("max_open_positions exceeded")

    entry_side = "buy" if signal > 0 else "sell"
    entry_price = _apply_slippage(float(market_event.open), entry_side, slippage)
    quantity = allocation / entry_price
    entry_fee = allocation * fee_rate
    if signal > 0:
        state["cash"] -= allocation + entry_fee
    else:
        state["cash"] += allocation - entry_fee
    positions[symbol] = {
        "direction": signal,
        "quantity": quantity,
        "entry_price": entry_price,
        "entry_time": market_event.timestamp,
        "entry_fee": entry_fee,
    }
    return state


__all__ = ["simulate_paper_order"]
