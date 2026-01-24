from decimal import Decimal, getcontext
from typing import Any, Dict

getcontext().prec = 12


def _clip_signal(s: Decimal) -> Decimal:
    if s > Decimal("1"):
        return Decimal("1")
    if s < Decimal("-1"):
        return Decimal("-1")
    return s


def score_alpha(
    signal: float,
    base_confidence: float,
    regime: str,
    regime_compatibility: Dict[str, float],
    regime_uncertainty: float = 0.0,
    uncertainty_penalty_weight: float = 0.5,
) -> Dict[str, Any]:
    """Compute explainable composite score for an alpha given current regime.

    Inputs:
      - signal: raw bounded signal in [-1,1] (float)
      - base_confidence: float in [0,1]
      - regime: current regime label
      - regime_compatibility: mapping regime -> multiplier (>=0, typical [0,2])
      - regime_uncertainty: [0,1] uncertainty in regime classification
      - uncertainty_penalty_weight: how strongly uncertainty reduces score

    Returns dict with components for auditability and explainability.
    """
    s = Decimal(str(signal))
    s = _clip_signal(s)
    base_conf = Decimal(str(max(0.0, min(1.0, base_confidence))))
    unc = Decimal(str(max(0.0, min(1.0, regime_uncertainty))))

    base_score = s  # preserves sign and magnitude

    # regime multiplier: default 1.0 if not listed
    regime_mult = Decimal(str(regime_compatibility.get(regime, 1.0)))

    # confidence penalty from regime uncertainty
    Decimal("1") - (Decimal("1") - unc) * Decimal(str(1 - uncertainty_penalty_weight))
    # we compute an uncertainty modifier in [1-uncertainty_penalty_weight, 1]
    uncertainty_modifier = Decimal("1") - unc * Decimal(str(uncertainty_penalty_weight))

    # composite multiplier
    composite_mult = (regime_mult * uncertainty_modifier * base_conf).quantize(
        Decimal("0.00000001")
    )

    final_score = (base_score * composite_mult).quantize(Decimal("0.00000001"))

    return {
        "base_score": base_score,
        "base_confidence": base_conf,
        "regime": regime,
        "regime_multiplier": regime_mult,
        "regime_uncertainty": unc,
        "uncertainty_modifier": uncertainty_modifier,
        "composite_multiplier": composite_mult,
        "final_score": final_score,
    }
