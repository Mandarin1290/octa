"""Promotion Engine — lifecycle state machine for model artifacts.

Orchestrates transitions between RESEARCH → SHADOW → PAPER → LIVE → RETIRED,
gating PAPER and LIVE promotions through the release and champion-challenger
decision functions.  All decisions (positive and negative) are emitted to the
governance audit chain.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

from .champion_challenger import decide_champion
from .governance_audit import (
    EVENT_MODEL_PROMOTED,
    EVENT_PROMOTION_REJECTED,
    GovernanceAudit,
)
from .model_release import decide_release

# ---------------------------------------------------------------------------
# State machine definition
# ---------------------------------------------------------------------------

_VALID_TRANSITIONS: Dict[str, set] = {
    "RESEARCH":    {"SHADOW", "PAPER"},   # PAPER direct for promote_model() compat
    "SHADOW":      {"PAPER", "RETIRED"},
    "PAPER":       {"LIVE", "RETIRED"},
    "LIVE":        {"RETIRED"},
    "QUARANTINED": {"RETIRED"},
    "RETIRED":     set(),                 # terminal
}

# Transitions into these states require eligibility gating.
_GATED_STATES = {"PAPER", "LIVE"}


# ---------------------------------------------------------------------------
# PromotionDecision
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PromotionDecision:
    ok: bool
    from_status: Optional[str]
    to_status: str
    artifact_id: int
    reason: str
    diagnostics: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# PromotionEngine
# ---------------------------------------------------------------------------

class PromotionEngine:
    """Lifecycle state-machine orchestrator.

    Parameters
    ----------
    registry : ArtifactRegistry
        Artifact registry (octa_ops.autopilot.registry.ArtifactRegistry).
    gov_audit : GovernanceAudit
        Governance audit chain instance.
    """

    def __init__(self, registry: Any, gov_audit: GovernanceAudit) -> None:
        self._registry = registry
        self._gov_audit = gov_audit

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def promote(
        self,
        artifact_id: int,
        to_status: str,
        *,
        scoring_report: Optional[Mapping[str, Any]] = None,
        validation_report: Optional[Mapping[str, Any]] = None,
        mc_report: Optional[Mapping[str, Any]] = None,
        thresholds: Optional[Mapping[str, Any]] = None,
        model_artifacts: Optional[Mapping[str, Any]] = None,
        champion_score: float = 0.0,
        reason: Optional[str] = None,
    ) -> PromotionDecision:
        """Attempt a lifecycle transition for *artifact_id* to *to_status*.

        Returns a :class:`PromotionDecision` — always non-raising; gate
        failures are captured as ``ok=False`` decisions with an audit event.
        """
        artifact_id = int(artifact_id)
        to_status = str(to_status)

        # 1. Resolve current status (None → "RESEARCH" for legacy rows)
        raw_status = self._registry.get_lifecycle_status(artifact_id)
        current_status = raw_status if raw_status is not None else "RESEARCH"

        # 2. Validate transition
        allowed = _VALID_TRANSITIONS.get(current_status, set())
        if to_status not in allowed:
            return self._reject(
                artifact_id=artifact_id,
                from_status=current_status,
                to_status=to_status,
                reason="invalid_transition",
                diagnostics={
                    "current": current_status,
                    "target": to_status,
                    "allowed": sorted(allowed),
                },
            )

        # 3. Eligibility gating for PAPER and LIVE
        if to_status in _GATED_STATES:
            release_dec = decide_release(
                validation_report if validation_report is not None else {},
                scoring_report if scoring_report is not None else {},
                mc_report if mc_report is not None else {},
                thresholds if thresholds is not None else {},
            )
            if not release_dec.released:
                return self._reject(
                    artifact_id=artifact_id,
                    from_status=current_status,
                    to_status=to_status,
                    reason=release_dec.reason,
                    diagnostics=dict(release_dec.diagnostics),
                )

            if to_status == "LIVE":
                artifacts = model_artifacts if model_artifacts is not None else {}
                challenger_score = float(artifacts.get("score", 0.0))
                stability_ok = bool(artifacts.get("stability_ok", False))
                min_improvement = float(
                    (thresholds or {}).get("min_improvement", 0.05)
                )
                champ_dec = decide_champion(
                    challenger_score=challenger_score,
                    champion_score=float(champion_score),
                    min_improvement=min_improvement,
                    stability_ok=stability_ok,
                )
                if not champ_dec.promote:
                    return self._reject(
                        artifact_id=artifact_id,
                        from_status=current_status,
                        to_status=to_status,
                        reason=champ_dec.reason,
                        diagnostics=dict(champ_dec.diagnostics),
                    )

        # 4. Apply the transition
        self._registry.set_lifecycle_status(artifact_id, to_status)

        # 5. Emit governance event
        self._gov_audit.emit(
            EVENT_MODEL_PROMOTED,
            {
                "artifact_id": artifact_id,
                "from_status": current_status,
                "to_status": to_status,
                "reason": reason or "promoted",
            },
        )

        return PromotionDecision(
            ok=True,
            from_status=current_status,
            to_status=to_status,
            artifact_id=artifact_id,
            reason=reason or "promoted",
        )

    def retire(
        self,
        artifact_id: int,
        *,
        reason: Optional[str] = None,
    ) -> PromotionDecision:
        """Convenience wrapper: transition *artifact_id* to RETIRED."""
        return self.promote(artifact_id, "RETIRED", reason=reason or "retired")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _reject(
        self,
        *,
        artifact_id: int,
        from_status: Optional[str],
        to_status: str,
        reason: str,
        diagnostics: Optional[Dict[str, Any]] = None,
    ) -> PromotionDecision:
        self._gov_audit.emit(
            EVENT_PROMOTION_REJECTED,
            {
                "artifact_id": artifact_id,
                "from_status": from_status,
                "to_status": to_status,
                "reason": reason,
            },
        )
        return PromotionDecision(
            ok=False,
            from_status=from_status,
            to_status=to_status,
            artifact_id=artifact_id,
            reason=reason,
            diagnostics=diagnostics or {},
        )
