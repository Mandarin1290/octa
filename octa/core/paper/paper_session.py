from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4


def start_paper_session(
    gate_result: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    if gate_result.get("status") != "PAPER_ELIGIBLE":
        raise ValueError("paper session start requires PAPER_ELIGIBLE gate status")

    summary = gate_result.get("summary", {})
    promotion_dir = summary.get("promotion_evidence_dir")
    shadow_dir = summary.get("shadow_evidence_dir")
    if not isinstance(promotion_dir, str) or not isinstance(shadow_dir, str):
        raise ValueError("gate_result summary missing referenced evidence directories")

    return {
        "status": "PAPER_SESSION_STARTED",
        "session_id": f"paper_session_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}",
        "start_time": datetime.now(timezone.utc).isoformat(),
        "promotion_evidence_dir": str(Path(promotion_dir).resolve()),
        "shadow_evidence_dir": str(Path(shadow_dir).resolve()),
        "paper_config": dict(config),
    }


__all__ = ["start_paper_session"]
