from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from octa.core.data.recycling.common import utc_now_compact
from octa.core.data.research_bridge import load_research_export
from octa.core.features.research_features import build_research_features
from octa.core.paper.paper_session_engine import run_paper_session
from octa.core.paper.paper_session_policy import PaperSessionPolicy
from octa.core.paper.paper_session_validation import validate_paper_session
from octa.core.paper.session_reporting import write_paper_session_evidence


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def run_paper_session_pipeline(
    *,
    paper_gate_evidence_dir: str | Path,
    market_data_adapter: Any,
    session_policy: PaperSessionPolicy | Mapping[str, Any],
    evidence_root: str | Path = "octa/var/evidence",
    run_id: str | None = None,
) -> dict[str, Any]:
    resolved_policy = (
        session_policy
        if isinstance(session_policy, PaperSessionPolicy)
        else PaperSessionPolicy.from_mapping(session_policy)
    )
    gate_dir = Path(paper_gate_evidence_dir)
    gate_report = _load_json(gate_dir / "paper_gate_report.json")
    gate_result = gate_report["gate_result"]
    promotion_evidence_dir = gate_report["promotion_evidence_dir"]

    resolved_run_id = run_id or f"paper_session_{utc_now_compact()}"
    evidence_dir = Path(evidence_root) / resolved_run_id

    promotion_report = _load_json(Path(promotion_evidence_dir) / "decision_report.json")
    shadow_evidence_dir = promotion_report["decision"]["summary"]["shadow_evidence_dir"]
    shadow_manifest = _load_json(Path(shadow_evidence_dir) / "run_manifest.json")
    research_export_path = shadow_manifest["research_export_path"]

    references = {
        "paper_gate_evidence_dir": str(gate_dir.resolve()),
        "promotion_evidence_dir": str(Path(promotion_evidence_dir).resolve()),
        "shadow_evidence_dir": str(Path(shadow_evidence_dir).resolve()),
        "research_export_path": str(Path(research_export_path).resolve()),
    }

    if gate_result.get("status") != resolved_policy.require_gate_status:
        paths = write_paper_session_evidence(
            evidence_dir=evidence_dir,
            references=references,
            session_policy=resolved_policy.to_dict(),
            session_result=None,
            validation_result=None,
            blocked_reason=f"gate_status={gate_result.get('status')}",
        )
        return {
            "status": "PAPER_BLOCKED",
            "report_path": paths["paper_session_report.json"],
            "evidence_dir": str(evidence_dir),
        }

    payload = load_research_export(research_export_path)
    signals_df = build_research_features(payload["signals"])
    session_result = run_paper_session(
        gate_result,
        market_data_adapter,
        signals_df,
        resolved_policy,
    )
    validation = validate_paper_session(
        gate_result,
        session_result,
        max_open_positions=resolved_policy.max_open_positions,
        kill_switch_drawdown=resolved_policy.kill_switch_drawdown,
    )
    paths = write_paper_session_evidence(
        evidence_dir=evidence_dir,
        references=references,
        session_policy=resolved_policy.to_dict(),
        session_result=session_result,
        validation_result=validation,
        blocked_reason=None,
    )
    return {
        "status": session_result["session_summary"]["status"],
        "report_path": paths["paper_session_report.json"],
        "evidence_dir": str(evidence_dir),
    }


__all__ = ["run_paper_session_pipeline"]
