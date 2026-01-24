from __future__ import annotations

import math
from typing import List

from octa_core.capacity import CapacityEngine, IneligibleAsset, LiquidityProfile


def vwap_slices(
    prof: LiquidityProfile,
    total_notional: float,
    engine: CapacityEngine,
    execution_window_hours: float = 6.5,
) -> List[float]:
    """Return a list of slice notionals (currency) for VWAP-style slicing.

    Slices are capped by `engine.compute_slice_limits` for the given execution window and
    rounded to tick_size where available.
    """
    try:
        slice_limit = engine.compute_slice_limits(
            prof, execution_window_hours=execution_window_hours
        )
    except IneligibleAsset:
        raise

    if slice_limit <= 0:
        return []

    # number of slices = ceil(total_notional / slice_limit)
    n = max(1, math.ceil(total_notional / slice_limit))
    base = total_notional / n
    slices = [base for _ in range(n)]

    # adjust for tick size (round down to nearest tick notional if tick_size provided)
    if prof.tick_size and prof.tick_size > 0:
        tick = prof.tick_size * prof.contract_multiplier * prof.price

        def round_tick(x: float) -> float:
            return math.floor(x / tick) * tick

        slices = [round_tick(s) for s in slices]
        # remove zero-sized slices
        slices = [s for s in slices if s > 0]

    return slices


__all__ = ["vwap_slices"]
