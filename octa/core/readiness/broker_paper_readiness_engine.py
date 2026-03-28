from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping

from .broker_paper_readiness_inventory import build_broker_paper_readiness_inventory
from .broker_paper_readiness_metrics import review_broker_paper_metrics
from .broker_paper_readiness_policy import BrokerPaperReadinessPolicy
from .broker_paper_readiness_validation import review_broker_paper_governance


def evaluate_broker_paper_readiness(
    evidence_roots: Mapping[str, Any] | Iterable[str | Path] | str | Path,
    policy: BrokerPaperReadinessPolicy | Mapping[str, Any],
) -> dict[str, Any]:
    resolved_policy = (
        policy if isinstance(policy, BrokerPaperReadinessPolicy) else BrokerPaperReadinessPolicy.from_mapping(policy)
    )
    inventory = build_broker_paper_readiness_inventory(evidence_roots)
    governance = review_broker_paper_governance(inventory)
    metrics = review_broker_paper_metrics(inventory, resolved_policy.metric_governance_policy)

    checks: list[dict[str, Any]] = []

    def add_check(name: str, passed: bool, value: Any, threshold: Any) -> None:
        checks.append(
            {
                "name": name,
                "status": "pass" if passed else "fail",
                "value": value,
                "threshold": threshold,
            }
        )

    add_check(
        "governance_integrity",
        (governance["status"] == "ok") if resolved_policy.require_governance_integrity else True,
        governance["status"],
        "ok" if resolved_policy.require_governance_integrity else "optional",
    )
    add_check(
        "negative_path_proof",
        bool(governance["summary"]["negative_path_proof"]) if resolved_policy.require_negative_path_proof else True,
        governance["summary"]["negative_path_proof"],
        True if resolved_policy.require_negative_path_proof else "optional",
    )
    add_check(
        "positive_path_proof",
        bool(governance["summary"]["positive_path_proof"]) if resolved_policy.require_positive_path_proof else True,
        governance["summary"]["positive_path_proof"],
        True if resolved_policy.require_positive_path_proof else "optional",
    )
    add_check(
        "broker_mode_paper_only",
        bool(governance["summary"]["paper_only_enforced"]) if resolved_policy.require_broker_mode_paper_only else True,
        governance["summary"]["paper_only_enforced"],
        True if resolved_policy.require_broker_mode_paper_only else "optional",
    )
    add_check(
        "kill_switch_path_tested",
        bool(governance["summary"]["kill_switch_path_tested"]) if resolved_policy.require_kill_switch_path_tested else True,
        governance["summary"]["kill_switch_path_tested"],
        True if resolved_policy.require_kill_switch_path_tested else "optional",
    )
    add_check(
        "no_live_flags",
        bool(governance["summary"]["no_live_flags"]) if resolved_policy.require_no_live_flags else True,
        governance["summary"]["no_live_flags"],
        True if resolved_policy.require_no_live_flags else "optional",
    )
    add_check(
        "evidence_chain_complete",
        bool(governance["summary"]["all_chain_complete"]) if resolved_policy.require_evidence_chain_complete else True,
        governance["summary"]["all_chain_complete"],
        True if resolved_policy.require_evidence_chain_complete else "optional",
    )
    add_check(
        "min_completed_broker_paper_sessions",
        int(metrics["summary"]["completed_broker_paper_sessions"]) >= resolved_policy.min_completed_broker_paper_sessions,
        int(metrics["summary"]["completed_broker_paper_sessions"]),
        resolved_policy.min_completed_broker_paper_sessions,
    )
    observed_drawdown = metrics["summary"]["max_observed_drawdown"]
    drawdown_ok = observed_drawdown is not None and float(observed_drawdown) <= resolved_policy.max_allowed_drawdown
    add_check(
        "max_allowed_drawdown",
        drawdown_ok,
        observed_drawdown,
        resolved_policy.max_allowed_drawdown,
    )
    add_check(
        "non_finite_metric_governance",
        metrics["summary"]["non_finite_metric_classification"] != "blocking",
        metrics["summary"]["non_finite_metric_classification"],
        "acceptable_with_caveat|normalized_with_flag",
    )

    status = "BROKER_PAPER_READY" if all(check["status"] == "pass" for check in checks) else "BROKER_PAPER_NOT_READY"
    return {
        "status": status,
        "checks": checks,
        "summary": {
            "policy": resolved_policy.to_dict(),
            "inventory_summary": inventory["summary"],
            "governance_summary": governance["summary"],
            "metrics_summary": metrics["summary"],
            "raw_metrics_snapshot": metrics["summary"]["raw_metrics_snapshot"],
            "normalized_metrics_snapshot": metrics["summary"]["normalized_metrics_snapshot"],
            "non_finite_flags": metrics["summary"]["non_finite_flags"],
            "normalization_annotations": metrics["summary"]["normalization_annotations"],
            "policy_decision_reason": metrics["summary"]["policy_decision_reason"],
            "risks": list(governance["summary"]["critical_risks"]) + list(metrics["summary"]["risks"]),
        },
        "inventory": inventory,
        "governance_report": governance,
        "metrics_report": metrics,
    }


__all__ = ["evaluate_broker_paper_readiness"]
