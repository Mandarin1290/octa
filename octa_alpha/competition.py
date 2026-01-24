from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, getcontext
from typing import Any, Dict, List

getcontext().prec = 28


def _quant(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class Submission:
    alpha_id: str
    requested_capital: Decimal
    expected_return: Decimal
    volatility: Decimal
    base_confidence: Decimal
    bid_price: Decimal = Decimal("0")
    metadata: Dict[str, Any] | None = None


def risk_adjusted_utility(
    sub: Submission, risk_aversion: Decimal = Decimal("1.0")
) -> Decimal:
    eps = Decimal("1e-12")
    denom = (sub.volatility * risk_aversion) + eps
    util = (sub.expected_return * sub.base_confidence) / denom
    return _quant(util)


def run_competition(
    submissions: List[Submission], total_capital: Decimal
) -> List[Dict[str, Any]]:
    """Run internal alpha competition.

    Rules:
    - Rank alphas by descending `utility` (risk-adjusted).
    - Tie-break: higher `bid_price` wins, then deterministic `alpha_id` ascending.
    - Allocate capital greedily to highest-ranked submissions until exhausted.
    - Partial fills allowed but no guaranteed allocations.

    Returns allocation records with explainable fields.
    """
    total_capital = _quant(total_capital)
    scored = []
    for s in submissions:
        util = risk_adjusted_utility(s)
        scored.append((s, util))

    # deterministic sort: utility desc, bid_price desc, alpha_id asc
    scored.sort(
        key=lambda t: (
            -float(t[1]),
            -float(t[0].bid_price),
            t[0].alpha_id,
        )
    )

    remaining = total_capital
    allocations: List[Dict[str, Any]] = []
    for sub, util in scored:
        req = _quant(sub.requested_capital)
        if remaining <= Decimal("0"):
            alloc = Decimal("0")
        else:
            alloc = req if req <= remaining else remaining
        alloc = _quant(alloc)
        remaining = _quant(remaining - alloc)
        allocations.append(
            {
                "alpha_id": sub.alpha_id,
                "requested_capital": req,
                "allocated_capital": alloc,
                "utility": util,
                "bid_price": _quant(sub.bid_price),
                "metadata": sub.metadata or {},
            }
        )

    # Keep returned order deterministic and by ranking (highest first)
    return allocations


__all__ = ["Submission", "risk_adjusted_utility", "run_competition"]
