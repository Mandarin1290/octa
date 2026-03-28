from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .broker_paper_policy import BrokerPaperPolicy
from .broker_paper_validation import validate_broker_paper_inputs


def evaluate_broker_paper_gate(
    paper_session_evidence_dir: str | Path,
    policy: BrokerPaperPolicy | Mapping[str, Any],
) -> dict[str, Any]:
    resolved_policy = (
        policy if isinstance(policy, BrokerPaperPolicy) else BrokerPaperPolicy.from_mapping(policy)
    )
    validation = validate_broker_paper_inputs(
        paper_session_evidence_dir,
        require_hash_integrity=resolved_policy.require_hash_integrity,
        max_session_age_hours=resolved_policy.max_session_age_hours,
        require_paper_gate_status=resolved_policy.require_paper_gate_status,
    )
    if not validation["is_valid"]:
        return {
            "status": "BROKER_PAPER_BLOCKED",
            "checks": validation["checks"],
            "summary": {
                "blocked_by_validation": True,
                "reason": validation["reason"],
                "paper_session_evidence_dir": str(Path(paper_session_evidence_dir).resolve()),
            },
        }

    checks = list(validation["checks"])
    metrics = validation["session_manifest"].get("metrics", {})
    session_status = validation["status"]
    completed_sessions = 1 if session_status == "PAPER_SESSION_COMPLETED" else 0

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
        "min_completed_sessions",
        completed_sessions >= resolved_policy.require_min_completed_sessions,
        completed_sessions,
        resolved_policy.require_min_completed_sessions,
    )
    add_check(
        "min_total_trades",
        int(metrics.get("n_trades", 0)) >= resolved_policy.require_min_total_trades,
        int(metrics.get("n_trades", 0)),
        resolved_policy.require_min_total_trades,
    )
    add_check(
        "min_win_rate",
        float(metrics.get("win_rate", 0.0)) >= resolved_policy.require_min_win_rate,
        float(metrics.get("win_rate", 0.0)),
        resolved_policy.require_min_win_rate,
    )
    add_check(
        "min_profit_factor",
        float(metrics.get("profit_factor", 0.0)) >= resolved_policy.require_min_profit_factor,
        float(metrics.get("profit_factor", 0.0)),
        resolved_policy.require_min_profit_factor,
    )
    drawdown = abs(float(metrics.get("max_drawdown", 1.0)))
    add_check("max_allowed_drawdown", drawdown <= resolved_policy.max_allowed_drawdown, drawdown, resolved_policy.max_allowed_drawdown)
    if resolved_policy.require_kill_switch_not_triggered:
        add_check(
            "kill_switch_not_triggered",
            bool(metrics.get("kill_switch_triggered", True)) is False,
            bool(metrics.get("kill_switch_triggered", True)),
            False,
        )
    add_check("broker_mode_required", resolved_policy.require_broker_mode == "PAPER", resolved_policy.require_broker_mode, "PAPER")
    add_check("forbid_live_mode", resolved_policy.forbid_live_mode is True, bool(resolved_policy.forbid_live_mode), True)

    status = "BROKER_PAPER_ELIGIBLE" if all(item["status"] == "pass" for item in checks) else "BROKER_PAPER_BLOCKED"
    return {
        "status": status,
        "checks": checks,
        "summary": {
            "blocked_by_validation": False,
            "paper_session_evidence_dir": str(Path(paper_session_evidence_dir).resolve()),
            "paper_gate_evidence_dir": validation["references"]["paper_gate_evidence_dir"],
            "promotion_evidence_dir": validation["references"]["promotion_evidence_dir"],
            "shadow_evidence_dir": validation["references"]["shadow_evidence_dir"],
            "research_export_path": validation["references"]["research_export_path"],
            "policy": resolved_policy.to_dict(),
            "passed_checks": sum(1 for item in checks if item["status"] == "pass"),
            "failed_checks": sum(1 for item in checks if item["status"] == "fail"),
        },
    }


__all__ = ["evaluate_broker_paper_gate"]
