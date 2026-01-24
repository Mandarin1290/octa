from dataclasses import dataclass
from typing import Callable, Dict


def inverse_scale_factory(reference_aum: float = 1_000_000.0, floor: float = 0.1):
    def scale(aum: float) -> float:
        if aum <= 0:
            return 1.0
        return max(floor, min(1.0, reference_aum / max(aum, reference_aum)))

    return scale


@dataclass
class StrategyCapacitySpec:
    base_fraction_of_aum: float  # e.g. 0.02 = 2% of AUM at reference
    scale_fn: Callable[[float], float]  # returns multiplier in (0,1]


class AUMAwareAllocator:
    """Allocator that is explicit about AUM when sizing targets.

    Key behaviour:
    - Compute proposed sizes from `expected_returns` (relative scores) and a `deploy_fraction`.
    - Compute per-strategy absolute capacity caps: `aum_total * base_fraction * scale_fn(aum_total)`.
    - Enforce capacity caps; reallocate leftover to non-capped strategies proportionally.
    - Guarantees no strategy exceeds its scalable capacity.
    """

    def __init__(
        self,
        capacity_specs: Dict[str, StrategyCapacitySpec],
        deploy_fraction: float = 0.8,
    ):
        self.capacity_specs = dict(capacity_specs)
        self.deploy_fraction = float(deploy_fraction)

    def allocate(
        self, expected_returns: Dict[str, float], aum_total: float
    ) -> Dict[str, float]:
        # sanitize inputs
        scores = {k: max(0.0, float(v)) for k, v in expected_returns.items()}
        # if all zeros, return zeros
        if not any(scores.values()):
            return {k: 0.0 for k in scores}

        # initial proposed absolute allocations based on scores
        total_score = sum(scores.values())
        deploy_budget = aum_total * self.deploy_fraction
        proposed = {k: (scores[k] / total_score) * deploy_budget for k in scores}

        # compute capacity caps
        caps: Dict[str, float] = {}
        for k in scores:
            spec = self.capacity_specs.get(k)
            if spec is None:
                # default conservative cap: 1% of AUM scaled by 1
                caps[k] = 0.01 * aum_total
            else:
                caps[k] = (
                    aum_total
                    * spec.base_fraction_of_aum
                    * max(0.0, min(1.0, spec.scale_fn(aum_total)))
                )

        allocated = {
            k: min(proposed.get(k, 0.0), caps.get(k, float("inf"))) for k in scores
        }

        # iterative reallocation of leftover budget to strategies not at cap
        remaining = deploy_budget - sum(allocated.values())
        unclamped = {k for k in scores if allocated[k] < caps.get(k, float("inf"))}
        iter_count = 0
        while remaining > 1e-8 and unclamped and iter_count < 10:
            iter_count += 1
            # distribute remaining to unclamped proportionally to scores among unclamped
            sub_scores = {k: scores[k] for k in unclamped}
            ssum = sum(sub_scores.values())
            if ssum == 0:
                break
            for k in list(unclamped):
                add = remaining * (sub_scores[k] / ssum)
                new_val = allocated[k] + add
                if new_val >= caps.get(k, float("inf")):
                    # hit cap, allocate only up to cap
                    add = caps[k] - allocated[k]
                    allocated[k] = caps[k]
                    unclamped.remove(k)
                    remaining -= max(0.0, add)
                else:
                    allocated[k] = new_val
                    remaining -= add
            # protect against floating rounding
            remaining = max(0.0, remaining)

        # final safety: ensure allocations <= caps
        for k in allocated:
            if allocated[k] > caps.get(k, float("inf")):
                allocated[k] = caps[k]

        return allocated
