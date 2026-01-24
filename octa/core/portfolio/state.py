from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class PortfolioState:
    positions: Mapping[str, float] = field(default_factory=dict)
    net_exposure: float = 0.0
    gross_exposure: float = 0.0
    sector_exposure: Mapping[str, float] = field(default_factory=dict)
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    portfolio_drawdown: float = 0.0
    rolling_volatility: float = 0.0
