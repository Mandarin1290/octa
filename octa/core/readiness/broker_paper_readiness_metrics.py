from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from .metric_normalization import normalize_readiness_metrics


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def review_broker_paper_metrics(
    inventory: dict[str, Any],
    metric_governance_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    broker_runs: list[dict[str, Any]] = []
    positive_runs: list[dict[str, Any]] = []
    blocked_runs: list[dict[str, Any]] = []
    risks: list[str] = []
    raw_metrics_snapshot: dict[str, Any] = {}
    normalized_metrics_snapshot: dict[str, Any] = {}
    non_finite_flags: list[str] = []
    normalization_annotations: list[str] = []
    normalization_decisions: list[dict[str, Any]] = []
    non_finite_metric_classification = "acceptable_with_caveat"
    policy_decision_reason = "no non-finite metrics detected"

    for chain in inventory.get("chains", []):
        broker_dir = Path(chain["broker_paper_evidence_dir"])
        report = _load_json(broker_dir / "broker_paper_report.json")
        session_summary = report.get("session_summary")
        if not isinstance(session_summary, dict):
            session_summary = {}
        gate_result = report.get("gate_result")
        if not isinstance(gate_result, dict):
            gate_result = {}
        entry: dict[str, Any] = {
            "broker_paper_evidence_dir": str(broker_dir.resolve()),
            "status": str(session_summary.get("status", gate_result.get("status", ""))),
            "blocked_reason": report.get("blocked_reason"),
        }
        if report.get("blocked_reason") is not None:
            entry["status"] = "BROKER_PAPER_BLOCKED"
            blocked_runs.append(entry)
            broker_runs.append(entry)
            continue

        metrics_path = broker_dir / "metrics.json"
        metrics = _load_json(metrics_path) if metrics_path.exists() else {}
        normalization = normalize_readiness_metrics(metrics, metric_governance_policy)
        raw_metrics_snapshot[broker_dir.name] = normalization["raw"]
        normalized_metrics_snapshot[broker_dir.name] = normalization["normalized"]
        non_finite_flags.extend(normalization["flags"])
        normalization_annotations.extend(normalization["annotations"])
        normalization_decisions.extend(
            [{"broker_paper_evidence_dir": str(broker_dir.resolve()), **decision} for decision in normalization["decisions"]]
        )
        if normalization["classification"] == "blocking":
            non_finite_metric_classification = "blocking"
            policy_decision_reason = normalization["policy_decision_reason"]
        elif (
            non_finite_metric_classification != "blocking"
            and normalization["classification"] == "normalized_with_flag"
        ):
            non_finite_metric_classification = "normalized_with_flag"
            policy_decision_reason = normalization["policy_decision_reason"]
        elif (
            non_finite_metric_classification == "acceptable_with_caveat"
            and normalization["policy_decision_reason"] != "all metrics finite or explicitly acceptable"
        ):
            policy_decision_reason = normalization["policy_decision_reason"]
        entry["metrics"] = metrics
        entry["normalized_metrics"] = normalization["normalized"]
        entry["normalization"] = normalization
        positive_runs.append(entry)
        broker_runs.append(entry)

    final_equities = [float(run["metrics"].get("final_equity", 0.0)) for run in positive_runs]
    max_drawdowns = [abs(float(run["metrics"].get("max_drawdown", 1.0))) for run in positive_runs]
    kill_switch_values = [bool(run["metrics"].get("kill_switch_triggered", True)) for run in positive_runs]
    n_orders = [int(run["metrics"].get("n_orders", 0)) for run in positive_runs]
    n_fills = [int(run["metrics"].get("n_fills", 0)) for run in positive_runs]
    n_trades = [int(run["metrics"].get("n_trades", 0)) for run in positive_runs]
    finite_metric_values = []
    for run in positive_runs:
        run_finite = True
        for key, value in run["metrics"].items():
            if isinstance(value, (int, float)) and not math.isfinite(float(value)):
                run_finite = False
                risks.append(f"Non-finite metric detected in {Path(run['broker_paper_evidence_dir']).name}: {key}={value}")
        finite_metric_values.append(run_finite)

    if len(positive_runs) < 2:
        risks.append("Positive broker-paper evidence base is smaller than two completed runs.")

    if len(positive_runs) >= 2:
        consistency = {
            "final_equity_range": max(final_equities) - min(final_equities),
            "max_drawdown_range": max(max_drawdowns) - min(max_drawdowns),
            "trade_count_range": max(n_trades) - min(n_trades),
        }
        consistency_status = "consistent" if consistency["trade_count_range"] == 0 else "variable"
    else:
        consistency = None
        consistency_status = "insufficient_data"

    return {
        "status": "ok",
        "summary": {
            "total_broker_paper_runs": len(broker_runs),
            "completed_broker_paper_sessions": len(positive_runs),
            "blocked_broker_paper_runs": len(blocked_runs),
            "session_statuses": [run["status"] for run in broker_runs],
            "final_equities": final_equities,
            "max_drawdowns": max_drawdowns,
            "kill_switch_triggered": kill_switch_values,
            "n_orders": n_orders,
            "n_fills": n_fills,
            "n_trades": n_trades,
            "max_observed_drawdown": max(max_drawdowns) if max_drawdowns else None,
            "consistency_status": consistency_status,
            "consistency": consistency,
            "finite_metric_values": finite_metric_values,
            "raw_metrics_snapshot": raw_metrics_snapshot,
            "normalized_metrics_snapshot": normalized_metrics_snapshot,
            "non_finite_flags": non_finite_flags,
            "normalization_annotations": normalization_annotations,
            "normalization_decisions": normalization_decisions,
            "non_finite_metric_classification": non_finite_metric_classification,
            "policy_decision_reason": policy_decision_reason,
            "risks": risks,
        },
        "runs": broker_runs,
    }


__all__ = ["review_broker_paper_metrics"]
