from __future__ import annotations

from datetime import timedelta
from typing import Any

import pandas as pd

from .market_data_adapter import MarketDataAdapter, PaperMarketEvent
from .paper_execution import simulate_paper_order
from .paper_session_policy import PaperSessionPolicy


def _mark_to_market(state: dict[str, Any], event: PaperMarketEvent) -> tuple[float, float]:
    unrealized = 0.0
    exposure = 0.0
    position = state["open_positions"].get(event.symbol)
    if position is not None:
        quantity = float(position["quantity"])
        entry_price = float(position["entry_price"])
        if int(position["direction"]) > 0:
            exposure = quantity * float(event.close)
            unrealized = (float(event.close) - entry_price) * quantity
        else:
            exposure = -quantity * float(event.close)
            unrealized = (entry_price - float(event.close)) * quantity
    state["unrealized_pnl"] = unrealized
    equity = float(state["cash"] + exposure)
    return equity, unrealized


def run_paper_session(
    gate_result: dict[str, Any],
    market_data_adapter: MarketDataAdapter,
    signals_df: pd.DataFrame,
    session_policy: PaperSessionPolicy | dict[str, Any],
) -> dict[str, Any]:
    resolved_policy = (
        session_policy
        if isinstance(session_policy, PaperSessionPolicy)
        else PaperSessionPolicy.from_mapping(session_policy)
    )
    if gate_result.get("status") != resolved_policy.require_gate_status:
        raise ValueError("paper session requires PAPER_ELIGIBLE gate result")
    if not isinstance(signals_df, pd.DataFrame):
        raise TypeError("signals_df must be a pandas DataFrame")
    if signals_df.empty:
        raise ValueError("signals_df must not be empty")
    if not isinstance(signals_df.index, pd.DatetimeIndex):
        raise TypeError("signals_df must use a DatetimeIndex")
    if not signals_df.index.is_monotonic_increasing:
        raise ValueError("signals_df index must be monotonic increasing")
    for name in ("long_signal", "short_signal"):
        if name not in signals_df.columns:
            raise ValueError(f"signals_df missing required column: {name}")

    state: dict[str, Any] = {
        "cash": float(resolved_policy.paper_capital),
        "open_positions": {},
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "trade_log": [],
        "peak_equity": float(resolved_policy.paper_capital),
        "kill_switch_triggered": False,
    }

    events = list(market_data_adapter.iter_events())
    if not events:
        raise ValueError("market_data_adapter returned no events")
    start_ts = events[0].timestamp
    max_end = start_ts + timedelta(minutes=resolved_policy.max_session_minutes)

    equity_rows: list[dict[str, Any]] = []
    event_timestamps = []
    for event in events:
        if event.timestamp > max_end:
            break
        event_timestamps.append(event.timestamp)
        if event.timestamp not in signals_df.index:
            raise ValueError(f"missing signal for event timestamp {event.timestamp.isoformat()}")

        signal_row = signals_df.loc[event.timestamp]
        long_signal = int(signal_row["long_signal"])
        short_signal = int(signal_row["short_signal"])
        if long_signal and short_signal:
            raise ValueError("simultaneous long and short signal in session input")
        target = 1 if long_signal else -1 if short_signal and resolved_policy.allow_short else 0
        state = simulate_paper_order(target, event, state, resolved_policy.to_dict())
        equity, unrealized = _mark_to_market(state, event)
        state["peak_equity"] = max(float(state["peak_equity"]), equity)
        drawdown = 0.0 if state["peak_equity"] <= 0 else max(0.0, 1.0 - (equity / float(state["peak_equity"])))
        if drawdown > float(resolved_policy.kill_switch_drawdown):
            state["kill_switch_triggered"] = True
            if event.symbol in state["open_positions"]:
                state = simulate_paper_order(0, event, state, resolved_policy.to_dict())
                equity, unrealized = _mark_to_market(state, event)
                drawdown = 0.0 if state["peak_equity"] <= 0 else max(0.0, 1.0 - (equity / float(state["peak_equity"])))
        equity_rows.append(
            {
                "timestamp": event.timestamp,
                "symbol": event.symbol,
                "cash": float(state["cash"]),
                "equity": float(equity),
                "realized_pnl": float(state["realized_pnl"]),
                "unrealized_pnl": float(unrealized),
                "open_positions": int(len(state["open_positions"])),
                "drawdown": float(drawdown),
                "kill_switch": bool(state["kill_switch_triggered"]),
            }
        )
        if state["kill_switch_triggered"]:
            break

    trades_df = pd.DataFrame(state["trade_log"])
    if not trades_df.empty:
        trades_df.index = range(1, len(trades_df) + 1)
        trades_df.index.name = "trade_id"
    equity_curve = pd.DataFrame(equity_rows).set_index("timestamp")
    final_equity = float(equity_curve["equity"].iloc[-1])
    session_metrics = {
        "final_equity": final_equity,
        "realized_pnl": float(state["realized_pnl"]),
        "unrealized_pnl": float(state["unrealized_pnl"]),
        "max_drawdown": float(equity_curve["drawdown"].max()) if not equity_curve.empty else 0.0,
        "kill_switch_triggered": bool(state["kill_switch_triggered"]),
        "n_trades": int(len(trades_df)),
        "event_count": int(len(equity_curve)),
    }
    session_summary = {
        "status": "PAPER_SESSION_COMPLETED",
        "start_time": events[0].timestamp.isoformat(),
        "end_time": equity_curve.index[-1].isoformat(),
        "symbols": sorted({event.symbol for event in events}),
        "event_timestamps": [ts.isoformat() for ts in event_timestamps],
    }
    return {
        "session_state": state,
        "trades": trades_df,
        "equity_curve": equity_curve,
        "metrics": session_metrics,
        "session_summary": session_summary,
    }


__all__ = ["run_paper_session"]
