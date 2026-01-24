from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Callable, Dict


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class PricingResult:
    strategy_id: str
    gross_return: float
    capital_used: float
    period_days: int
    capital_charge: float
    penalty_multiplier: float
    net_return: float


class InternalPricing:
    """Applies internal cost of capital to strategy performance.

    - `hurdle_rate` is annualized (e.g., 0.10 for 10% p.a.).
    - `penalty_multiplier` is applied to the capital charge if a strategy's net return would be negative,
      penalizing inefficient strategies.
    - `audit_fn` is invoked with `internal_pricing.charge` payloads.
    """

    def __init__(
        self,
        hurdle_rate: float = 0.10,
        penalty_multiplier: float = 1.5,
        audit_fn: Callable[[str, Dict[str, Any]], None] | None = None,
    ):
        self.hurdle_rate = float(hurdle_rate)
        self.penalty_multiplier = float(penalty_multiplier)
        self.audit_fn = audit_fn or (lambda e, p: None)

    def _annualized_charge(self, capital: float, period_days: int) -> float:
        # simple pro-rata annual charge
        return float(capital) * self.hurdle_rate * (float(period_days) / 365.0)

    def apply_charges(
        self,
        gross_returns: Dict[str, float],
        capital_used: Dict[str, float],
        period_days: int = 365,
    ) -> Dict[str, PricingResult]:
        results: Dict[str, PricingResult] = {}
        for sid, gross in gross_returns.items():
            cap = float(capital_used.get(sid, 0.0))
            charge = self._annualized_charge(cap, period_days)
            # provisional net
            net = float(gross) - charge
            penalty_mult = 1.0
            if net < 0:
                # penalize inefficient strategies by increasing their capital charge
                penalty_mult = self.penalty_multiplier
                charge = charge * penalty_mult
                net = float(gross) - charge

            res = PricingResult(
                strategy_id=sid,
                gross_return=float(gross),
                capital_used=cap,
                period_days=int(period_days),
                capital_charge=charge,
                penalty_multiplier=penalty_mult,
                net_return=net,
            )
            self.audit_fn("internal_pricing.charge", asdict(res))
            results[sid] = res

        return results
