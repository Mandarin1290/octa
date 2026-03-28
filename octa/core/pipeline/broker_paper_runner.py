from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from octa.core.data.recycling.common import utc_now_compact
from octa.core.data.research_bridge import load_research_export
from octa.core.features.research_features import build_research_features

from octa.core.broker_paper.broker_paper_gate import evaluate_broker_paper_gate
from octa.core.broker_paper.broker_paper_policy import BrokerPaperPolicy
from octa.core.broker_paper.broker_paper_session import run_broker_paper_session
from octa.core.broker_paper.broker_paper_session_validation import validate_broker_paper_session
from octa.core.broker_paper.reporting import write_broker_paper_evidence


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def run_broker_paper(
    *,
    paper_session_evidence_dir: str | Path,
    policy: BrokerPaperPolicy | Mapping[str, Any],
    market_data_adapter: Any | None = None,
    broker_adapter: Any | None = None,
    evidence_root: str | Path = "octa/var/evidence",
    run_id: str | None = None,
) -> dict[str, Any]:
    resolved_policy = (
        policy if isinstance(policy, BrokerPaperPolicy) else BrokerPaperPolicy.from_mapping(policy)
    )
    gate_result = evaluate_broker_paper_gate(paper_session_evidence_dir, resolved_policy)
    paper_session_manifest = _load_json(Path(paper_session_evidence_dir) / "session_manifest.json")
    references = {
        "paper_session_evidence_dir": str(Path(paper_session_evidence_dir).resolve()),
        **paper_session_manifest["references"],
    }

    resolved_run_id = run_id or f"broker_paper_{utc_now_compact()}"
    evidence_dir = Path(evidence_root) / resolved_run_id

    if gate_result["status"] != "BROKER_PAPER_ELIGIBLE":
        paths = write_broker_paper_evidence(
            evidence_dir=evidence_dir,
            references=references,
            policy=resolved_policy.to_dict(),
            gate_result=gate_result,
            session_result=None,
            validation_result=None,
            blocked_reason=f"gate_status={gate_result['status']}",
        )
        return {
            "status": "BROKER_PAPER_BLOCKED",
            "report_path": paths["broker_paper_report.json"],
            "evidence_dir": str(evidence_dir),
        }

    if market_data_adapter is None or broker_adapter is None:
        raise ValueError("eligible broker paper run requires both market_data_adapter and broker_adapter")

    payload = load_research_export(references["research_export_path"])
    signals_df = build_research_features(payload["signals"])
    session_result = run_broker_paper_session(
        gate_result,
        market_data_adapter,
        broker_adapter,
        signals_df,
        resolved_policy,
    )
    validation = validate_broker_paper_session(
        session_result,
        require_broker_mode=resolved_policy.require_broker_mode,
        max_open_positions=resolved_policy.max_open_positions,
        kill_switch_drawdown=resolved_policy.kill_switch_drawdown,
    )
    paths = write_broker_paper_evidence(
        evidence_dir=evidence_dir,
        references=references,
        policy=resolved_policy.to_dict(),
        gate_result=gate_result,
        session_result=session_result,
        validation_result=validation,
        blocked_reason=None,
    )
    return {
        "status": session_result["session_status"],
        "report_path": paths["broker_paper_report.json"],
        "evidence_dir": str(evidence_dir),
    }


__all__ = ["run_broker_paper"]
