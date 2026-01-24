from __future__ import annotations

from typing import Dict

from .slippage import post_trade_realized, pre_trade_slippage_estimate


def estimate_trade_cost(order: Dict, market: Dict, params: Dict | None = None) -> Dict:
    """High-level wrapper to estimate trade cost.

    order: {size, price, ts}
    market: {adv, sigma, half_spread}
    params: {impact_coeff, fixed_fees}
    """
    p = params or {}
    return pre_trade_slippage_estimate(
        size=order.get("size", 0.0),
        price=order.get("price", 0.0),
        adv=market.get("adv", 0.0),
        sigma=market.get("sigma", 0.0),
        half_spread=market.get("half_spread", 0.0),
        ts=order.get("ts", ""),
        impact_coeff=p.get("impact_coeff", 0.1),
        fixed_fees=p.get("fixed_fees", 0.0),
    )


def realized_trade_cost(
    order: Dict, market: Dict, realized_slippage_pct: float, params: Dict | None = None
) -> Dict:
    p = params or {}
    return post_trade_realized(
        size=order.get("size", 0.0),
        price=order.get("price", 0.0),
        adv=market.get("adv", 0.0),
        realized_slippage_pct=realized_slippage_pct,
        half_spread=market.get("half_spread", 0.0),
        ts=order.get("ts", ""),
        fixed_fees=p.get("fixed_fees", 0.0),
    )
