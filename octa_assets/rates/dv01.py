from typing import List, Tuple

from octa_assets.rates.duration import BondSpec


def bond_dv01(bond: BondSpec, notional: float, price: float = 1.0) -> float:
    """Estimate DV01 for a bond position.

    DV01 approximated as: modified_duration * notional * price * 0.0001
    If only duration is provided (not modified), treat it as modified for conservative estimate.
    """
    md = bond.modified_duration if bond.modified_duration is not None else bond.duration
    if md is None:
        raise ValueError("No duration available for DV01 calculation")
    return float(md) * float(notional) * float(price) * 0.0001


def aggregate_dv01(
    positions: List[Tuple[BondSpec, float, float]], cap: float | None = None
) -> float:
    """Aggregate DV01 across positions.

    positions: list of (BondSpec, notional, price)
    If a bond has no duration, it's treated conservatively by applying `cap` if provided; otherwise raises.
    """
    total = 0.0
    for bond, notional, price in positions:
        try:
            dv = bond_dv01(bond, notional, price)
        except ValueError:
            if cap is not None:
                dv = cap
            else:
                raise
        total += dv
    return total
