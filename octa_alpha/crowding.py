from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, getcontext
from math import sqrt
from typing import Any, Dict, List, Tuple

getcontext().prec = 28


def _quant(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class CrowdingProfile:
    alpha_id: str
    exposure: List[Decimal]
    metadata: Dict[str, Any] | None = None


def _cosine_similarity(a: List[Decimal], b: List[Decimal]) -> Decimal:
    af = [float(x) for x in a]
    bf = [float(x) for x in b]
    num = sum(x * y for x, y in zip(af, bf, strict=False))
    n1 = sqrt(sum(x * x for x in af))
    n2 = sqrt(sum(y * y for y in bf))
    if n1 == 0 or n2 == 0:
        return Decimal("0")
    sim = Decimal(str(num / (n1 * n2)))
    if sim < 0:
        sim = Decimal("0")
    return _quant(sim)


def pairwise_overlaps(profiles: List[CrowdingProfile]) -> Dict[str, Dict[str, Decimal]]:
    overlaps: Dict[str, Dict[str, Decimal]] = {}
    for i, p in enumerate(profiles):
        overlaps[p.alpha_id] = {}
        for j, q in enumerate(profiles):
            if i == j:
                overlaps[p.alpha_id][q.alpha_id] = Decimal("1")
            else:
                overlaps[p.alpha_id][q.alpha_id] = _cosine_similarity(
                    p.exposure, q.exposure
                )
    return overlaps


def crowding_index(profiles: List[CrowdingProfile]) -> Dict[str, Decimal]:
    overlaps = pairwise_overlaps(profiles)
    indices: Dict[str, Decimal] = {}
    for p in profiles:
        others = [v for k, v in overlaps[p.alpha_id].items() if k != p.alpha_id]
        if not others:
            indices[p.alpha_id] = Decimal("0")
        else:
            avg = sum(float(x) for x in others) / len(others)
            indices[p.alpha_id] = _quant(Decimal(str(avg)))
    return indices


def diminishing_multiplier(
    crowd: Decimal,
    threshold: Decimal = Decimal("0.5"),
    exponent: Decimal = Decimal("2.0"),
) -> Decimal:
    """Returns a multiplier in (0,1] that shrinks with crowding.

    Uses formula: mult = 1 / (1 + (crowd/threshold)**exponent)
    """
    if threshold <= Decimal("0"):
        denom = (crowd**exponent) + Decimal("1")
        mult = Decimal("1") / denom
    else:
        ratio = crowd / threshold
        denom = Decimal("1") + (ratio**exponent)
        mult = Decimal("1") / denom
    # ensure between 0 and 1
    if mult > Decimal("1"):
        mult = Decimal("1")
    if mult < Decimal("0"):
        mult = Decimal("0")
    return _quant(mult)


def apply_crowding_penalties(
    base_util: Dict[str, Decimal],
    profiles: List[CrowdingProfile],
    threshold: Decimal = Decimal("0.5"),
    exponent: Decimal = Decimal("2.0"),
) -> Tuple[Dict[str, Decimal], Dict[str, Decimal]]:
    """Return (adjusted_util, multipliers) mapping alpha_id -> Decimal."""
    indices = crowding_index(profiles)
    adjusted: Dict[str, Decimal] = {}
    multipliers: Dict[str, Decimal] = {}
    for p in profiles:
        base = base_util.get(p.alpha_id, Decimal("0"))
        mult = diminishing_multiplier(indices[p.alpha_id], threshold, exponent)
        adjusted[p.alpha_id] = _quant(base * mult)
        multipliers[p.alpha_id] = mult
    return adjusted, multipliers


__all__ = [
    "CrowdingProfile",
    "pairwise_overlaps",
    "crowding_index",
    "diminishing_multiplier",
    "apply_crowding_penalties",
]
