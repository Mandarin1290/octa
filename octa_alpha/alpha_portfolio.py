from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, getcontext
from typing import Any, Dict, List

from octa_alpha.crowding import (
    CrowdingProfile,
    apply_crowding_penalties,
    pairwise_overlaps,
)

getcontext().prec = 28


def _quant(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class AlphaCandidate:
    alpha_id: str
    base_utility: Decimal
    volatility: Decimal
    exposure: List[Decimal]
    metadata: Dict[str, Any] | None = None


def optimize_weights(
    candidates: List[AlphaCandidate],
    total_risk_budget: Decimal = Decimal("0.1"),
    max_per_alpha: Decimal = Decimal("0.3"),
    behavior_threshold: Decimal = Decimal("0.8"),
    max_behavior_share: Decimal = Decimal("0.5"),
    crowding_threshold: Decimal = Decimal("0.5"),
    crowding_exponent: Decimal = Decimal("2.0"),
) -> Dict[str, Decimal]:
    """Return optimized weights mapping alpha_id -> weight (sums to 1 or 0 if no positive utility).

    Simple deterministic heuristic optimizer:
    1. Apply crowding penalties to base utilities.
    2. Initial weights proportional to adjusted utilities (negatives -> 0).
    3. Enforce `max_per_alpha` (cap and redistribute).
    4. Enforce behavior diversification: clusters by overlap > behavior_threshold limited to `max_behavior_share`.
    5. Enforce total risk budget by scaling weights down if needed.
    """
    if not candidates:
        return {}

    # build crowding profiles
    profiles = [
        CrowdingProfile(alpha_id=c.alpha_id, exposure=c.exposure) for c in candidates
    ]

    base_util = {c.alpha_id: _quant(c.base_utility) for c in candidates}
    adjusted_util, _mults = apply_crowding_penalties(
        base_util, profiles, threshold=crowding_threshold, exponent=crowding_exponent
    )

    # zero-out negatives
    for k, v in list(adjusted_util.items()):
        if v <= Decimal("0"):
            adjusted_util[k] = Decimal("0")

    total_util = sum(float(v) for v in adjusted_util.values())
    weights: Dict[str, Decimal] = {}
    if total_util <= 0:
        # fallback: equal weights for all
        n = len(candidates)
        for c in candidates:
            weights[c.alpha_id] = _quant(Decimal("1") / Decimal(n))
    else:
        for c in candidates:
            weights[c.alpha_id] = _quant(
                Decimal(str(float(adjusted_util[c.alpha_id]) / total_util))
            )

    # enforce max per alpha
    def cap_and_redistribute(w: Dict[str, Decimal], cap: Decimal) -> Dict[str, Decimal]:
        # iterative cap + proportional redistribution among uncapped
        w = {k: _quant(v) for k, v in w.items()}
        # iterate until no weight exceeds cap (or no where to redistribute)
        for _ in range(10):
            over = [k for k, v in w.items() if v > cap]
            if not over:
                break
            excess = sum((w[k] - cap) for k in over)
            for k in over:
                w[k] = _quant(cap)
            others = [k for k, v in w.items() if v < cap]
            if not others:
                break
            total_others = sum(float(w[k]) for k in others)
            if total_others <= 0:
                break
            for k in others:
                prop = float(w[k]) / total_others
                w[k] = _quant(w[k] + Decimal(str(prop)) * excess)
        # final normalize
        s = sum(w.values())
        if s == 0:
            return {k: Decimal("0") for k in w}
        return {k: _quant(v / s) for k, v in w.items()}

    # enforce per-alpha cap via scaled proportional allocation using binary search
    def cap_via_scale(utils: Dict[str, Decimal], cap: Decimal) -> Dict[str, Decimal]:
        # utils are non-negative, return weights that sum to 1 and each <= cap
        u_vals = {k: float(v) for k, v in utils.items()}
        if all(v == 0.0 for v in u_vals.values()):
            n = len(u_vals)
            return {k: _quant(Decimal("1") / Decimal(n)) for k in u_vals}

        low = 0.0
        high = 1.0

        # increase high until sum(min(high * u, cap)) >= 1
        def mass(k: float) -> float:
            return sum(min(k * u_vals[kid], float(cap)) for kid in u_vals)

        while mass(high) < 1.0:
            high *= 2.0
            if high > 1e12:
                break

        for _ in range(60):
            mid = (low + high) / 2.0
            m = mass(mid)
            if m > 1.0:
                high = mid
            else:
                low = mid

        k = (low + high) / 2.0
        raw = {kid: Decimal(str(min(k * u_vals[kid], float(cap)))) for kid in u_vals}
        s = sum(raw.values())
        if s == 0:
            return {kid: Decimal("0") for kid in raw}
        # If total possible weight under caps is < 1, return raw (unallocated slack allowed).
        if s < 1:
            return {kid: _quant(v) for kid, v in raw.items()}
        # otherwise normalize to sum=1
        return {kid: _quant(v / s) for kid, v in raw.items()}

    weights = cap_via_scale(adjusted_util, max_per_alpha)

    # behavior clusters
    overlaps = pairwise_overlaps(profiles)
    # build clusters greedily: any pair with overlap > threshold in same cluster
    clusters: List[set] = []
    for c in candidates:
        placed = False
        for cl in clusters:
            if any(overlaps[c.alpha_id][other] >= behavior_threshold for other in cl):
                cl.add(c.alpha_id)
                placed = True
                break
        if not placed:
            clusters.append({c.alpha_id})

    # enforce cluster share cap
    for cl in clusters:
        cl_share = sum(weights[k] for k in cl)
        if cl_share > max_behavior_share:
            # scale down cluster weights proportionally
            scale = _quant(max_behavior_share / cl_share)
            for k in cl:
                weights[k] = _quant(weights[k] * scale)
            # redistribute leftover proportionally to others outside cluster
            from decimal import Decimal as _Decimal

            deficit = _quant(_Decimal("1") - sum(weights.values(), _Decimal("0")))
            others = [k for k in weights if k not in cl]
            if others and deficit > Decimal("0"):
                total_others = sum(float(weights[k]) for k in others)
                for k in others:
                    prop = float(weights[k]) / total_others if total_others > 0 else 0
                    weights[k] = _quant(weights[k] + Decimal(str(prop)) * deficit)

    # enforce risk budget: sum(weight * volatility) <= total_risk_budget
    port_risk = sum(weights[c.alpha_id] * c.volatility for c in candidates)
    if port_risk > total_risk_budget and port_risk > Decimal("0"):
        scale = _quant(total_risk_budget / port_risk)
        for k in weights:
            weights[k] = _quant(weights[k] * scale)
        # renormalize to sum to <=1 (it will be <=1)
        s = sum(weights.values())
        if s > 0:
            for k in weights:
                weights[k] = _quant(weights[k] / s)

    # final normalization: only normalize if total weight > 1 (we allow unallocated slack)
    s = sum(weights.values())
    if s == 0:
        return {k.alpha_id: Decimal("0") for k in candidates}
    if s > Decimal("1"):
        for k in weights:
            weights[k] = _quant(weights[k] / s)

    return weights


__all__ = ["AlphaCandidate", "optimize_weights"]
