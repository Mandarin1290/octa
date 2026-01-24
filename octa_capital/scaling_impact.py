from dataclasses import dataclass
from datetime import datetime
from math import exp
from typing import Any, Callable, Dict, List, Optional


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class ScalingResult:
    aum: float
    expected_return: float
    marginal_return_per_unit: float


class ScalingImpactAnalyzer:
    """Conservative estimator for return degradation as AUM scales.

    Model: adjusted_return = base_return * exp(-beta * (aum / base_aum - 1))
    - `beta` controls conservativeness (higher -> faster degradation).
    - Provides marginal return per AUM unit and break-even AUM where return_rate <= hurdle_rate.
    """

    def __init__(
        self,
        beta: float = 0.5,
        audit_fn: Callable[[str, Dict[str, Any]], None] | None = None,
    ):
        self.beta = float(beta)
        self.audit_fn = audit_fn or (lambda e, p: None)

    def _base_return(self, historical_returns: List[float]) -> float:
        if not historical_returns:
            return 0.0
        return float(sum(historical_returns) / len(historical_returns))

    def simulate_scaling(
        self, historical_returns: List[float], base_aum: float, target_aums: List[float]
    ) -> List[ScalingResult]:
        base_return = self._base_return(historical_returns)
        results: List[ScalingResult] = []
        prev_expected = None
        prev_a = None
        for a in sorted(target_aums):
            factor = a / float(base_aum)
            expected = base_return * exp(-self.beta * (factor - 1.0))
            if prev_expected is None:
                marginal = expected / max(a, 1e-12)
            else:
                marginal = (expected - prev_expected) / max((a - prev_a), 1e-12)
            results.append(
                ScalingResult(
                    aum=a, expected_return=expected, marginal_return_per_unit=marginal
                )
            )
            prev_expected = expected
            prev_a = a
        # audit summary
        self.audit_fn(
            "scaling_impact.simulation",
            {
                "base_return": base_return,
                "beta": self.beta,
                "samples": len(historical_returns),
            },
        )
        return results

    def compute_break_even(
        self,
        historical_returns: List[float],
        base_aum: float,
        hurdle_rate: float,
        search_aum_max: float = 1e9,
        step: float = 1_000.0,
    ) -> Optional[float]:
        base_return = self._base_return(historical_returns)
        # we consider return rate per AUM unit = expected_return / aum
        a = base_aum
        while a <= search_aum_max:
            expected = base_return * exp(-self.beta * (a / base_aum - 1.0))
            rate = expected / max(a, 1e-12)
            if rate <= hurdle_rate:
                self.audit_fn(
                    "scaling_impact.break_even",
                    {"break_even_aum": a, "hurdle_rate": hurdle_rate},
                )
                return a
            a += step
        return None
