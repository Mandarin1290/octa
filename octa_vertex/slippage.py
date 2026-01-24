from __future__ import annotations

import math
from datetime import datetime
from typing import Dict


def time_of_day_factor(ts: str) -> float:
    """Deterministic liquidity factor by hour of day (UTC).

    Returns >=1.0 where >1 increases cost (less liquidity).
    Simple piecewise schedule: open and close windows are more costly.
    """
    try:
        dt = datetime.fromisoformat(ts)
        hour = dt.hour
    except Exception:
        # if parsing fails, return neutral
        return 1.0

    # quiet midday (10-15 UTC) slightly cheaper
    if 10 <= hour < 15:
        return 0.9
    # open/close windows costlier
    if 7 <= hour < 10 or 15 <= hour < 18:
        return 1.2
    # overnight illiquidity
    if hour < 7 or hour >= 18:
        return 1.5
    return 1.0


def sqrt_impact(
    size: float, adv: float, sigma: float, impact_coeff: float = 0.1
) -> float:
    """Square-root impact model.

    - size: trade size in same units as ADV (e.g., shares or notional)
    - adv: average daily volume (same units)
    - sigma: daily volatility (as decimal, e.g., 0.02 for 2%)
    - impact_coeff: calibrated coefficient (deterministic)

    Returns impact as fraction of price (e.g., 0.001 = 0.1%).
    """
    if adv <= 0 or size <= 0:
        return 0.0
    v = size / adv
    # sqrt law scaled by volatility and coefficient
    return impact_coeff * sigma * math.sqrt(v)


def pre_trade_slippage_estimate(
    size: float,
    price: float,
    adv: float,
    sigma: float,
    half_spread: float,
    ts: str,
    impact_coeff: float = 0.1,
    fixed_fees: float = 0.0,
) -> Dict[str, float]:
    """Estimate pre-trade costs. Returns breakdown in currency units.

    - half_spread: in price units (e.g., 0.01 USD)
    - price: current mid price
    - fixed_fees: absolute fixed fees per trade
    """
    tof = time_of_day_factor(ts)
    impact_pct = sqrt_impact(size, adv, sigma, impact_coeff) * tof
    spread_cost = half_spread * size
    impact_cost = impact_pct * price * size
    total = fixed_fees + spread_cost + impact_cost
    return {
        "fixed_fees": fixed_fees,
        "spread_cost": spread_cost,
        "impact_cost": impact_cost,
        "total_estimate": total,
        "impact_pct": impact_pct,
    }


def post_trade_realized(
    size: float,
    price: float,
    adv: float,
    realized_slippage_pct: float,
    half_spread: float,
    ts: str,
    fixed_fees: float = 0.0,
) -> Dict[str, float]:
    """Compute realized costs given realized slippage percent (from execution)."""
    tof = time_of_day_factor(ts)
    spread_cost = half_spread * size
    realized_impact = realized_slippage_pct * price * size * tof
    total = fixed_fees + spread_cost + realized_impact
    return {
        "fixed_fees": fixed_fees,
        "spread_cost": spread_cost,
        "realized_impact": realized_impact,
        "total": total,
    }
