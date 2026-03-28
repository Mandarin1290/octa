from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from octa.core.data.recycling.common import utc_now_compact
from octa.core.paper.paper_gate import evaluate_paper_gate
from octa.core.paper.paper_policy import PaperPolicy
from octa.core.paper.paper_session import start_paper_session
from octa.core.paper.reporting import write_paper_reports


def run_paper_gate(
    *,
    promotion_evidence_dir: str | Path,
    policy: PaperPolicy | Mapping[str, Any],
    evidence_root: str | Path = "octa/var/evidence",
    run_id: str | None = None,
    start_session: bool = False,
) -> dict[str, Any]:
    resolved_policy = (
        policy if isinstance(policy, PaperPolicy) else PaperPolicy.from_mapping(policy)
    )
    gate_result = evaluate_paper_gate(promotion_evidence_dir, resolved_policy)

    session_manifest = None
    if start_session:
        session_manifest = start_paper_session(gate_result, resolved_policy.to_dict())

    resolved_run_id = run_id or f"paper_gate_{utc_now_compact()}"
    evidence_dir = Path(evidence_root) / resolved_run_id
    report_paths = write_paper_reports(
        evidence_dir=evidence_dir,
        promotion_evidence_dir=promotion_evidence_dir,
        policy=resolved_policy.to_dict(),
        gate_result=gate_result,
        session_manifest=session_manifest,
    )
    return {
        "status": session_manifest["status"] if session_manifest is not None else gate_result["status"],
        "gate_status": gate_result["status"],
        "report_path": report_paths["paper_gate_report.json"],
        "evidence_dir": str(evidence_dir),
    }


__all__ = ["run_paper_gate"]
