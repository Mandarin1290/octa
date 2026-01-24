from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class NavSnapshot:
    ts: str
    base_currency: str
    nav: float
    pnl_realized: float
    pnl_unrealized: float


def compute_nav_and_pnl(
    *,
    ts: str,
    base_currency: str,
    positions: Dict[str, float],
    prices: Dict[str, float],
    cost_basis: Optional[Dict[str, float]] = None,
    realized_pnl: float = 0.0,
) -> NavSnapshot:
    """Compute a simple NAV/PnL snapshot.

    - positions: symbol -> quantity
    - prices: symbol -> price
    - cost_basis: symbol -> average entry price (optional)

    Deterministic and intentionally conservative.
    """

    nav = 0.0
    unreal = 0.0
    cb = cost_basis or {}
    for sym, qty in positions.items():
        px = float(prices.get(sym, 0.0))
        nav += float(qty) * px
        if sym in cb:
            unreal += float(qty) * (px - float(cb[sym]))

    return NavSnapshot(
        ts=str(ts),
        base_currency=str(base_currency),
        nav=float(nav),
        pnl_realized=float(realized_pnl),
        pnl_unrealized=float(unreal),
    )


__all__ = ["NavSnapshot", "compute_nav_and_pnl"]
