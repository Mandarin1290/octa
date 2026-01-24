from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, getcontext
from typing import Any, Dict, Tuple

from octa_alpha.regime_scoring import score_alpha

getcontext().prec = 28


def _quant(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


@dataclass
class RegimeTransitionEngine:
    dampening_factor: Decimal = Decimal(
        "0.5"
    )  # fraction to reduce exposure on transition
    uncertainty_increase: Decimal = Decimal("0.2")  # additive uncertainty on transition
    compression_periods: int = 2  # temporary risk compression duration

    def detect_transition(self, prev_regime: str, curr_regime: str) -> bool:
        return prev_regime != curr_regime

    def handle_transition(
        self,
        prev_regime: str,
        curr_regime: str,
        current_exposure: Decimal,
        regime_uncertainty: Decimal,
    ) -> Tuple[Decimal, Decimal, int]:
        """Return (new_exposure, new_uncertainty, compression_periods_remaining).

        - If regimes differ, exposure is dampened by `dampening_factor` and uncertainty increased.
        - If regimes same, values preserved.
        """
        current_exposure = _quant(current_exposure)
        regime_uncertainty = _quant(regime_uncertainty)
        if not self.detect_transition(prev_regime, curr_regime):
            return current_exposure, regime_uncertainty, 0

        # apply dampening
        damp = max(Decimal("0"), min(Decimal("1"), self.dampening_factor))
        new_exposure = _quant(current_exposure * (Decimal("1") - damp))

        # increase uncertainty
        new_unc = regime_uncertainty + self.uncertainty_increase
        if new_unc > Decimal("1"):
            new_unc = Decimal("1")
        new_unc = _quant(new_unc)

        return new_exposure, new_unc, self.compression_periods

    def re_evaluate_score(
        self,
        signal: float,
        base_confidence: float,
        regime: str,
        regime_compatibility: Dict[str, float],
        regime_uncertainty: float,
        uncertainty_penalty_weight: float = 0.5,
    ) -> Dict[str, Any]:
        """Recompute explainable score using `score_alpha`.

        This is a thin wrapper so callers can pass float/Decimal interchangeably.
        """
        return score_alpha(
            signal,
            base_confidence,
            regime,
            regime_compatibility,
            regime_uncertainty,
            uncertainty_penalty_weight,
        )


__all__ = ["RegimeTransitionEngine"]
