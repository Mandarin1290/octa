from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from octa.core.data.recycling.common import utc_now_compact
from octa.core.promotion.promotion_engine import evaluate_promotion
from octa.core.promotion.promotion_policy import PromotionPolicy
from octa.core.promotion.reporting import write_promotion_reports


def run_promotion(
    *,
    shadow_evidence_dir: str | Path,
    policy: PromotionPolicy | Mapping[str, Any],
    evidence_root: str | Path = "octa/var/evidence",
    run_id: str | None = None,
) -> dict[str, Any]:
    resolved_policy = (
        policy if isinstance(policy, PromotionPolicy) else PromotionPolicy.from_mapping(policy)
    )
    decision = evaluate_promotion(shadow_evidence_dir, resolved_policy)
    resolved_run_id = run_id or f"promotion_run_{utc_now_compact()}"
    evidence_dir = Path(evidence_root) / resolved_run_id
    report_paths = write_promotion_reports(
        evidence_dir=evidence_dir,
        input_evidence_dir=shadow_evidence_dir,
        policy=resolved_policy.to_dict(),
        decision=decision,
    )
    return {
        "status": decision["status"],
        "report_path": report_paths["decision_report.json"],
        "evidence_dir": str(evidence_dir),
    }


__all__ = ["run_promotion"]
