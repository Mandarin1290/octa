from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional

from octa_alpha.alpha_portfolio import AlphaCandidate, optimize_weights
from octa_alpha.crowding import CrowdingProfile, crowding_index
from octa_alpha.governance import Governance
from octa_alpha.regime_scoring import score_alpha


@dataclass
class StrategyFactoryMaster:
    governance: Optional[Governance] = None

    def alpha_inventory(self, candidates: List[AlphaCandidate]) -> List[Dict[str, Any]]:
        inv = []
        for c in candidates:
            inv.append(
                {
                    "alpha_id": c.alpha_id,
                    "base_utility": c.base_utility,
                    "volatility": c.volatility,
                    "exposure": c.exposure,
                    "metadata": c.metadata or {},
                    "governance_state": (
                        self.governance.states.get(c.alpha_id)
                        if self.governance
                        else None
                    ),
                }
            )
        return inv

    def regime_adjusted_scores(
        self,
        signals: Dict[str, float],
        base_conf: Dict[str, float],
        regime: str,
        regime_compatibility: Dict[str, Dict[str, float]],
        regime_uncertainty: Dict[str, float],
    ) -> Dict[str, Dict[str, Any]]:
        scores = {}
        for aid, sig in signals.items():
            conf = base_conf.get(aid, 1.0)
            compat = regime_compatibility.get(aid, {})
            unc = regime_uncertainty.get(aid, 0.0)
            scores[aid] = score_alpha(sig, conf, regime, compat, unc)
        return scores

    def allocation_map(
        self, candidates: List[AlphaCandidate], **opt_kwargs
    ) -> Dict[str, Decimal]:
        weights = optimize_weights(candidates, **opt_kwargs)
        return weights

    def crowding_indicators(
        self, candidates: List[AlphaCandidate]
    ) -> Dict[str, Decimal]:
        profiles = [
            CrowdingProfile(alpha_id=c.alpha_id, exposure=c.exposure)
            for c in candidates
        ]
        return crowding_index(profiles)

    def governance_interventions(self) -> Dict[str, Any]:
        if not self.governance:
            return {"audit_log": [], "vetoed": []}
        return {
            "audit_log": self.governance.get_audit(),
            "vetoed": [k for k, v in self.governance.states.items() if v == "vetoed"],
        }

    def build_dashboard(
        self,
        candidates: List[AlphaCandidate],
        signals: Dict[str, float],
        base_conf: Dict[str, float],
        regime: str,
        regime_compatibility: Dict[str, Dict[str, float]],
        regime_uncertainty: Dict[str, float],
        opt_kwargs: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        opt_kwargs = opt_kwargs or {}
        inv = self.alpha_inventory(candidates)
        scores = self.regime_adjusted_scores(
            signals, base_conf, regime, regime_compatibility, regime_uncertainty
        )
        alloc = self.allocation_map(candidates, **opt_kwargs)
        crowd = self.crowding_indicators(candidates)
        gov = self.governance_interventions()

        # reconcile checks (deterministic) — include simple reconcilable sums
        total_weight = sum(w for w in alloc.values()) if alloc else Decimal("0")

        return {
            "alpha_inventory": inv,
            "regime_scores": scores,
            "allocation_map": alloc,
            "total_weight": total_weight,
            "crowding": crowd,
            "governance": gov,
        }


__all__ = ["StrategyFactoryMaster"]
