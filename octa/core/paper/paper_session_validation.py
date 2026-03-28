from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def validate_paper_session(
    gate_result: dict[str, Any],
    session_result: dict[str, Any],
    *,
    max_open_positions: int,
    kill_switch_drawdown: float,
) -> dict[str, Any]:
    if gate_result.get("status") != "PAPER_ELIGIBLE":
        raise ValueError("paper session validation requires PAPER_ELIGIBLE gate result")

    trades_df = session_result["trades"]
    equity_curve = session_result["equity_curve"]
    session_state = session_result["session_state"]

    if equity_curve.empty:
        raise ValueError("equity_curve must not be empty")
    if equity_curve.isna().any().any():
        raise ValueError("equity_curve contains NaNs")
    if not isinstance(equity_curve.index, pd.DatetimeIndex):
        raise TypeError("equity_curve must use a DatetimeIndex")
    if not equity_curve.index.is_monotonic_increasing:
        raise ValueError("equity_curve index must be monotonic increasing")

    if int(equity_curve["open_positions"].max()) > max_open_positions:
        raise ValueError("max_open_positions exceeded in session result")
    if not np.isfinite(pd.to_numeric(equity_curve["equity"], errors="raise").to_numpy(dtype=float)).all():
        raise ValueError("equity_curve contains non-finite equity")
    if float(equity_curve["drawdown"].max()) > kill_switch_drawdown and not bool(equity_curve["kill_switch"].max()):
        raise ValueError("kill-switch drawdown breach not enforced")
    if len(session_state["open_positions"]) > max_open_positions:
        raise ValueError("session_state open_positions inconsistent")
    if not trades_df.empty:
        if trades_df.isna().any().any():
            raise ValueError("trades_df contains NaNs")
        allowed_ts = set(equity_curve.index)
        entry_times = set(pd.to_datetime(trades_df["entry_time"], utc=True))
        exit_times = set(pd.to_datetime(trades_df["exit_time"], utc=True))
        if not entry_times.issubset(allowed_ts) or not exit_times.issubset(allowed_ts):
            raise ValueError("trade timestamps must be subset of market event timestamps")

    return {
        "status": "ok",
        "n_trades": int(len(trades_df)),
        "n_equity_rows": int(len(equity_curve)),
        "kill_switch_triggered": bool(equity_curve["kill_switch"].max()),
    }


__all__ = ["validate_paper_session"]
