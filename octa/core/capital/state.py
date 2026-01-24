from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CapitalState:
    total_equity: float
    free_equity: float
    used_margin: float
    open_positions: int
    net_exposure: float
    gross_exposure: float
    realized_pnl: float
    unrealized_pnl: float
