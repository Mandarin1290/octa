from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, getcontext
from math import sqrt
from typing import Any, Dict, List

from octa_alpha.competition import Submission, risk_adjusted_utility

getcontext().prec = 28


def _quant(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class AlphaProfile:
    alpha_id: str
    requested_capital: Decimal
    expected_return: Decimal
    volatility: Decimal
    base_confidence: Decimal
    exposure: List[Decimal]
    metadata: Dict[str, Any] | None = None


def _cosine_similarity(a: List[Decimal], b: List[Decimal]) -> Decimal:
    # convert to floats for stability of math.sqrt, but keep Decimal quantization for output
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


def detect_overlaps(profiles: List[AlphaProfile]) -> Dict[str, Dict[str, Decimal]]:
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


def resolve_arbitration(
    profiles: List[AlphaProfile],
    total_capital: Decimal,
    overlap_threshold: Decimal = Decimal("0.8"),
) -> List[Dict[str, Any]]:
    total_capital = _quant(total_capital)
    overlaps = detect_overlaps(profiles)

    # base utilities
    util_map = {
        p.alpha_id: risk_adjusted_utility(
            Submission(
                alpha_id=p.alpha_id,
                requested_capital=p.requested_capital,
                expected_return=p.expected_return,
                volatility=p.volatility,
                base_confidence=p.base_confidence,
            )
        )
        for p in profiles
    }

    # sort profiles by base utility desc then alpha_id asc for determinism
    sorted_profiles = sorted(
        profiles, key=lambda p: (-float(util_map[p.alpha_id]), p.alpha_id)
    )

    remaining = total_capital
    allocations: List[Dict[str, Any]] = []
    selected: List[AlphaProfile] = []

    for p in sorted_profiles:
        # compute max overlap with already selected
        max_ov = Decimal("0")
        for s in selected:
            ov = overlaps[p.alpha_id][s.alpha_id]
            if ov > max_ov:
                max_ov = ov

        # if overlap exceeds threshold, consider it redundant and defund
        if max_ov >= overlap_threshold:
            alloc = Decimal("0")
        else:
            # reduce requested capital by crowding factor (max overlap)
            adjusted_req = _quant(p.requested_capital * (Decimal("1") - max_ov))
            if remaining <= Decimal("0"):
                alloc = Decimal("0")
            else:
                alloc = adjusted_req if adjusted_req <= remaining else remaining
            alloc = _quant(alloc)

        allocations.append(
            {
                "alpha_id": p.alpha_id,
                "requested_capital": _quant(p.requested_capital),
                "allocated_capital": alloc,
                "max_overlap_with_selected": max_ov,
                "base_utility": util_map[p.alpha_id],
                "metadata": p.metadata or {},
            }
        )

        if alloc > Decimal("0"):
            selected.append(p)
            remaining = _quant(remaining - alloc)

    return allocations


__all__ = ["AlphaProfile", "detect_overlaps", "resolve_arbitration"]
