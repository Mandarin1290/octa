from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def validate_broker_paper_session(
    session_result: dict[str, Any],
    *,
    require_broker_mode: str,
    max_open_positions: int,
    kill_switch_drawdown: float,
) -> dict[str, Any]:
    if require_broker_mode != "PAPER":
        raise ValueError("broker paper validation requires PAPER mode")
    if session_result["summary"]["status"] not in {"BROKER_PAPER_SESSION_COMPLETED", "BROKER_PAPER_SESSION_ABORTED"}:
        raise ValueError("invalid broker paper session status")
    for key in ("orders", "fills", "positions", "equity_curve"):
        frame = session_result[key]
        if not isinstance(frame, pd.DataFrame):
            raise TypeError(f"{key} must be a pandas DataFrame")
        if not frame.empty and frame.isna().any().any():
            raise ValueError(f"{key} contains NaNs")

    equity_curve = session_result["equity_curve"]
    if equity_curve.empty:
        raise ValueError("equity_curve must not be empty")
    if float(equity_curve["drawdown"].max()) > kill_switch_drawdown and not bool(equity_curve["kill_switch"].max()):
        raise ValueError("kill-switch drawdown breach not enforced")
    positions = session_result["positions"]
    if not positions.empty and int(positions["open_positions"].max()) > max_open_positions:
        raise ValueError("max_open_positions exceeded")
    fills = session_result["fills"]
    if not fills.empty and not fills["mode"].eq("PAPER").all():
        raise ValueError("non-PAPER fill detected")
    orders = session_result["orders"]
    if not orders.empty and not orders["mode"].eq("PAPER").all():
        raise ValueError("non-PAPER order detected")
    if not np.isfinite(pd.to_numeric(equity_curve["equity"], errors="raise").to_numpy(dtype=float)).all():
        raise ValueError("non-finite equity detected")
    return {
        "status": "ok",
        "n_orders": int(len(orders)),
        "n_fills": int(len(fills)),
        "n_equity_rows": int(len(equity_curve)),
        "kill_switch_triggered": bool(equity_curve["kill_switch"].max()),
    }


__all__ = ["validate_broker_paper_session"]
