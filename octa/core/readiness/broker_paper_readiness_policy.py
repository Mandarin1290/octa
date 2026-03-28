from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Mapping

from .metric_governance_policy import default_metric_governance_policy, resolve_metric_governance_policy


@dataclass(frozen=True)
class BrokerPaperReadinessPolicy:
    require_governance_integrity: bool
    require_negative_path_proof: bool
    require_positive_path_proof: bool
    require_broker_mode_paper_only: bool
    min_completed_broker_paper_sessions: int
    max_allowed_drawdown: float
    require_kill_switch_path_tested: bool
    require_no_live_flags: bool
    require_evidence_chain_complete: bool
    metric_governance_policy: dict[str, Any] = field(default_factory=default_metric_governance_policy)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "BrokerPaperReadinessPolicy":
        required = (
            "require_governance_integrity",
            "require_negative_path_proof",
            "require_positive_path_proof",
            "require_broker_mode_paper_only",
            "min_completed_broker_paper_sessions",
            "max_allowed_drawdown",
            "require_kill_switch_path_tested",
            "require_no_live_flags",
            "require_evidence_chain_complete",
        )
        missing = [key for key in required if key not in payload]
        if missing:
            raise ValueError(f"broker paper readiness policy missing required keys: {missing}")
        return cls(
            require_governance_integrity=bool(payload["require_governance_integrity"]),
            require_negative_path_proof=bool(payload["require_negative_path_proof"]),
            require_positive_path_proof=bool(payload["require_positive_path_proof"]),
            require_broker_mode_paper_only=bool(payload["require_broker_mode_paper_only"]),
            min_completed_broker_paper_sessions=int(payload["min_completed_broker_paper_sessions"]),
            max_allowed_drawdown=float(payload["max_allowed_drawdown"]),
            require_kill_switch_path_tested=bool(payload["require_kill_switch_path_tested"]),
            require_no_live_flags=bool(payload["require_no_live_flags"]),
            require_evidence_chain_complete=bool(payload["require_evidence_chain_complete"]),
            metric_governance_policy=resolve_metric_governance_policy(
                payload.get("metric_governance_policy")
            ),
        )


__all__ = ["BrokerPaperReadinessPolicy"]
