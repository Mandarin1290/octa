from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from octa.core.data.recycling.common import utc_now_compact
from octa.core.operations.broker_paper_ops_engine import execute_broker_paper_ops
from octa.core.operations.broker_paper_ops_metrics import aggregate_broker_paper_ops_metrics
from octa.core.operations.broker_paper_ops_planner import plan_broker_paper_runs
from octa.core.operations.broker_paper_ops_policy import BrokerPaperOpsPolicy
from octa.core.operations.reporting import write_broker_paper_ops_evidence


def run_broker_paper_ops(
    *,
    readiness_evidence_dir: str | Path,
    policy: BrokerPaperOpsPolicy | Mapping[str, Any],
    evidence_root: str | Path = "octa/var/evidence",
    run_id: str | None = None,
) -> dict[str, Any]:
    resolved_policy = (
        policy if isinstance(policy, BrokerPaperOpsPolicy) else BrokerPaperOpsPolicy.from_mapping(policy)
    )
    plan = plan_broker_paper_runs(readiness_evidence_dir, resolved_policy)
    plan_payload = {**plan, "policy": resolved_policy.to_dict()}
    resolved_run_id = run_id or f"broker_paper_ops_{utc_now_compact()}"
    evidence_dir = Path(evidence_root) / resolved_run_id

    if plan["status"] != "OPS_PLAN_READY":
        execution = {
            "batch_status": "OPS_BLOCKED",
            "runs": [],
            "summary": {"reason": plan["summary"]["reason"]},
        }
        aggregated_metrics = aggregate_broker_paper_ops_metrics([])
        paths = write_broker_paper_ops_evidence(
            evidence_dir=evidence_dir,
            plan=plan_payload,
            execution=execution,
            aggregated_metrics=aggregated_metrics,
        )
        return {
            "status": "OPS_BLOCKED",
            "evidence_dir": str(evidence_dir),
            "report_path": paths["ops_report.json"],
        }

    execution = execute_broker_paper_ops(
        plan_payload,
        evidence_root=evidence_root,
        batch_run_id=resolved_run_id,
    )
    aggregated_metrics = execution["summary"]["aggregated_metrics"]
    paths = write_broker_paper_ops_evidence(
        evidence_dir=evidence_dir,
        plan=plan_payload,
        execution=execution,
        aggregated_metrics=aggregated_metrics,
    )
    return {
        "status": execution["batch_status"],
        "evidence_dir": str(evidence_dir),
        "report_path": paths["ops_report.json"],
    }


__all__ = ["run_broker_paper_ops"]
