"""Strategy Health Scorer: aggregates diagnostics into an explainable health score.

Health is in [0,1], where 1.0 = healthy. Inputs are component reports (alpha_decay,
regime_fit, stability, drawdown profile, risk_util). No single metric may
contribute more than `max_contribution` (default 0.4) to ensure no dominance.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class HealthReport:
    score: float
    components: Dict[str, float]
    contributions: Dict[str, float]
    explain: Dict[str, Any]


class HealthScorer:
    def __init__(
        self, weights: Optional[Dict[str, float]] = None, max_contribution: float = 0.4
    ):
        # default weights (must sum >0)
        self.weights = weights or {
            "alpha": 0.25,
            "regime": 0.2,
            "stability": 0.25,
            "drawdown": 0.15,
            "risk": 0.15,
        }
        self.max_contribution = float(max_contribution)

    @staticmethod
    def _clamp01(x: float) -> float:
        try:
            return max(0.0, min(1.0, float(x)))
        except Exception:
            return 0.0

    @staticmethod
    def _drawdown_penalty(profile: Optional[Dict[str, Any]]) -> float:
        # map classifications to penalty in [0,1] where 1 worst
        if not profile or "classification" not in profile:
            return 0.0
        cls = profile["classification"]
        mapping = {
            "NONE": 0.0,
            "LONG_SHALLOW": 0.2,
            "MIXED": 0.3,
            "CLUSTERED": 0.45,
            "QUICK_RECOVERY": 0.05,
            "SHARP_CRASH": 0.8,
        }
        return mapping.get(cls, 0.3)

    def _normalize_weights(self) -> Dict[str, float]:
        # cap individual weights to max_contribution, renormalize remaining
        w = dict(self.weights)
        # clamp negative
        for k in list(w.keys()):
            if w[k] < 0:
                w[k] = 0.0
        total = sum(w.values())
        if total == 0:
            # fallback equal weights
            keys = list(w.keys())
            return {k: 1.0 / len(keys) for k in keys}
        # normalize to sum 1
        for k in w:
            w[k] = w[k] / total

        # cap and redistribute
        capped = {k: min(self.max_contribution, v) for k, v in w.items()}
        remaining = 1.0 - sum(capped.values())
        if remaining <= 0:
            # renormalize capped to sum 1
            s = sum(capped.values())
            return {k: v / s for k, v in capped.items()} if s > 0 else capped
        # redistribute remaining proportionally to those not at cap
        free_keys = [k for k, v in w.items() if v < self.max_contribution]
        if not free_keys:
            s = sum(capped.values())
            return {k: v / s for k, v in capped.items()} if s > 0 else capped
        free_sum = sum(w[k] for k in free_keys)
        for k in free_keys:
            capped[k] += remaining * (
                w[k] / (free_sum if free_sum > 0 else len(free_keys))
            )
        # final safety normalize
        s = sum(capped.values())
        if s <= 0:
            return capped
        return {k: v / s for k, v in capped.items()}

    def score(
        self,
        *,
        alpha_decay: Optional[Dict[str, Any]] = None,
        regime_fit: Optional[Dict[str, Any]] = None,
        stability: Optional[Dict[str, Any]] = None,
        drawdown_profile: Optional[Dict[str, Any]] = None,
        risk_util: Optional[float] = None,
    ) -> HealthReport:
        # compute component healths in [0,1] where 1 is healthy
        # alpha_decay: expects {'decay_score': 0..1}
        alpha_decay_score = (
            float(alpha_decay.get("decay_score") or 0.0)
            if isinstance(alpha_decay, dict) and "decay_score" in alpha_decay
            else 0.0
        )
        alpha_health = 1.0 - self._clamp01(alpha_decay_score)

        # regime_fit: expects {'compatibility_score': 0..1}
        regime_score = (
            float(regime_fit.get("compatibility_score") or 0.5)
            if isinstance(regime_fit, dict) and "compatibility_score" in regime_fit
            else 0.5
        )
        regime_health = self._clamp01(regime_score)

        # stability: expects {'stability_score': 0..1} (higher = more unstable)
        stability_score = (
            float(stability.get("stability_score") or 0.0)
            if isinstance(stability, dict) and "stability_score" in stability
            else 0.0
        )
        stability_health = 1.0 - self._clamp01(stability_score)

        # drawdown profile: map to penalty
        profile_raw = (
            drawdown_profile.get("profile")
            if isinstance(drawdown_profile, dict)
            else None
        )
        profile: Dict[str, Any] | None = (
            profile_raw if isinstance(profile_raw, dict) else None
        )
        dd_penalty = self._drawdown_penalty(profile)
        drawdown_health = 1.0 - self._clamp01(dd_penalty)

        # risk_util: expects numeric max util fraction (e.g. 1.2) where >1 is breach
        ru = float(risk_util) if risk_util is not None else 0.0
        ru_clamped = min(1.0, max(0.0, ru))
        risk_health = 1.0 - ru_clamped

        components = {
            "alpha": self._clamp01(alpha_health),
            "regime": self._clamp01(regime_health),
            "stability": self._clamp01(stability_health),
            "drawdown": self._clamp01(drawdown_health),
            "risk": self._clamp01(risk_health),
        }

        weights = self._normalize_weights()

        contributions = {
            k: components.get(k, 0.0) * weights.get(k, 0.0) for k in components
        }
        score = sum(contributions.values())

        explain = {
            "raw": {
                "alpha_decay_score": alpha_decay_score,
                "regime_score": regime_score,
                "stability_score": stability_score,
                "drawdown_penalty": dd_penalty,
                "risk_util_raw": ru,
            },
            "weights": weights,
            "components": components,
            "contributions": contributions,
        }

        return HealthReport(
            score=self._clamp01(score),
            components=components,
            contributions=contributions,
            explain=explain,
        )
