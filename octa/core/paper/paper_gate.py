from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .paper_policy import PaperPolicy
from .paper_validation import validate_promotion_evidence


def evaluate_paper_gate(
    promotion_evidence_dir: str | Path,
    policy: PaperPolicy | Mapping[str, Any],
) -> dict[str, Any]:
    resolved_policy = (
        policy if isinstance(policy, PaperPolicy) else PaperPolicy.from_mapping(policy)
    )
    validation = validate_promotion_evidence(
        promotion_evidence_dir,
        require_hash_integrity=resolved_policy.require_hash_integrity,
        require_recent_promotion=resolved_policy.require_recent_promotion,
        max_promotion_age_hours=resolved_policy.max_promotion_age_hours,
        require_shadow_metrics_present=resolved_policy.require_shadow_metrics_present,
    )
    if not validation["is_valid"]:
        return {
            "status": "PAPER_BLOCKED",
            "checks": validation["checks"],
            "summary": {
                "blocked_by_validation": True,
                "reason": validation["reason"],
                "promotion_evidence_dir": str(Path(promotion_evidence_dir).resolve()),
            },
        }

    checks = list(validation["checks"])
    promotion_status = validation["decision_report"]["decision"]["status"]
    checks.append(
        {
            "name": "require_promotion_status",
            "status": "pass" if promotion_status == resolved_policy.require_promotion_status else "fail",
            "value": promotion_status,
            "threshold": resolved_policy.require_promotion_status,
        }
    )
    status = "PAPER_ELIGIBLE" if all(item["status"] == "pass" for item in checks) else "PAPER_BLOCKED"
    return {
        "status": status,
        "checks": checks,
        "summary": {
            "blocked_by_validation": False,
            "promotion_evidence_dir": str(Path(promotion_evidence_dir).resolve()),
            "shadow_evidence_dir": validation["shadow_evidence_dir"],
            "policy": resolved_policy.to_dict(),
            "passed_checks": sum(1 for item in checks if item["status"] == "pass"),
            "failed_checks": sum(1 for item in checks if item["status"] == "fail"),
        },
    }


__all__ = ["evaluate_paper_gate"]
