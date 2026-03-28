from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from octa.core.data.recycling.common import sha256_file


REQUIRED_PAPER_SESSION_FILES = (
    "session_manifest.json",
    "paper_session_report.json",
    "session_policy.json",
    "evidence_manifest.json",
)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON in {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def validate_broker_paper_inputs(
    paper_session_evidence_dir: str | Path,
    *,
    require_hash_integrity: bool,
    max_session_age_hours: float,
    require_paper_gate_status: str,
) -> dict[str, Any]:
    evidence_dir = Path(paper_session_evidence_dir)
    checks: list[dict[str, Any]] = []

    if not evidence_dir.exists() or not evidence_dir.is_dir():
        return {
            "is_valid": False,
            "checks": [{
                "name": "paper_session_evidence_dir_exists",
                "status": "fail",
                "value": str(evidence_dir),
                "threshold": "existing_directory",
            }],
            "reason": "missing_paper_session_evidence_dir",
        }

    for name in REQUIRED_PAPER_SESSION_FILES:
        exists = (evidence_dir / name).exists()
        checks.append(
            {
                "name": f"required_file:{name}",
                "status": "pass" if exists else "fail",
                "value": exists,
                "threshold": True,
            }
        )
        if not exists:
            return {"is_valid": False, "checks": checks, "reason": f"missing_required_file:{name}"}

    try:
        session_manifest = _load_json(evidence_dir / "session_manifest.json")
        session_report = _load_json(evidence_dir / "paper_session_report.json")
        session_policy = _load_json(evidence_dir / "session_policy.json")
        evidence_manifest = _load_json(evidence_dir / "evidence_manifest.json")
    except ValueError as exc:
        checks.append(
            {
                "name": "json_parse_integrity",
                "status": "fail",
                "value": str(exc),
                "threshold": "valid_json",
            }
        )
        return {"is_valid": False, "checks": checks, "reason": "invalid_json"}

    hashes = evidence_manifest.get("hashes", {})
    if not isinstance(hashes, dict):
        return {"is_valid": False, "checks": checks, "reason": "invalid_evidence_manifest_hashes"}

    if require_hash_integrity:
        for rel_name, expected_hash in sorted(hashes.items()):
            path = evidence_dir / rel_name
            exists = path.exists()
            checks.append(
                {
                    "name": f"hash_file_exists:{rel_name}",
                    "status": "pass" if exists else "fail",
                    "value": exists,
                    "threshold": True,
                }
            )
            if not exists:
                return {"is_valid": False, "checks": checks, "reason": f"missing_hashed_file:{rel_name}"}
            actual_hash = sha256_file(path)
            ok = actual_hash == expected_hash
            checks.append(
                {
                    "name": f"hash_integrity:{rel_name}",
                    "status": "pass" if ok else "fail",
                    "value": actual_hash,
                    "threshold": expected_hash,
                }
            )
            if not ok:
                return {"is_valid": False, "checks": checks, "reason": f"hash_mismatch:{rel_name}"}

    blocked_reason = session_report.get("blocked_reason")
    session_summary = session_report.get("session_summary")
    if blocked_reason is not None:
        status = "PAPER_BLOCKED"
    elif isinstance(session_summary, dict):
        status = str(session_summary.get("status", ""))
    else:
        status = ""
    checks.append(
        {
            "name": "paper_session_status_present",
            "status": "pass" if status in {"PAPER_BLOCKED", "PAPER_SESSION_COMPLETED", "PAPER_SESSION_ABORTED"} else "fail",
            "value": status,
            "threshold": "PAPER_BLOCKED|PAPER_SESSION_COMPLETED|PAPER_SESSION_ABORTED",
        }
    )
    if status not in {"PAPER_BLOCKED", "PAPER_SESSION_COMPLETED", "PAPER_SESSION_ABORTED"}:
        return {"is_valid": False, "checks": checks, "reason": "invalid_paper_session_status"}

    refs = session_report.get("references", {})
    if not isinstance(refs, dict):
        return {"is_valid": False, "checks": checks, "reason": "missing_references"}
    required_refs = (
        "paper_gate_evidence_dir",
        "promotion_evidence_dir",
        "shadow_evidence_dir",
        "research_export_path",
    )
    for key in required_refs:
        ref_path = refs.get(key)
        ok = isinstance(ref_path, str) and Path(ref_path).exists()
        checks.append(
            {
                "name": f"reference_exists:{key}",
                "status": "pass" if ok else "fail",
                "value": ref_path,
                "threshold": "existing_path",
            }
        )
        if not ok:
            return {"is_valid": False, "checks": checks, "reason": f"missing_reference:{key}"}

    paper_gate_report = _load_json(Path(refs["paper_gate_evidence_dir"]) / "paper_gate_report.json")
    gate_status = paper_gate_report["gate_result"]["status"]
    checks.append(
        {
            "name": "paper_gate_status_valid",
            "status": "pass" if gate_status in {"PAPER_BLOCKED", "PAPER_ELIGIBLE"} else "fail",
            "value": gate_status,
            "threshold": "PAPER_BLOCKED|PAPER_ELIGIBLE",
        }
    )
    if gate_status not in {"PAPER_BLOCKED", "PAPER_ELIGIBLE"}:
        return {"is_valid": False, "checks": checks, "reason": "invalid_paper_gate_status"}
    checks.append(
        {
            "name": "required_paper_gate_status",
            "status": "pass" if gate_status == require_paper_gate_status else "fail",
            "value": gate_status,
            "threshold": require_paper_gate_status,
        }
    )

    age_hours = (
        datetime.now(timezone.utc).timestamp() - (evidence_dir / "paper_session_report.json").stat().st_mtime
    ) // 1 / 3600.0
    checks.append(
        {
            "name": "paper_session_age_hours",
            "status": "pass" if age_hours <= max_session_age_hours else "fail",
            "value": age_hours,
            "threshold": max_session_age_hours,
        }
    )
    if age_hours > max_session_age_hours:
        return {"is_valid": False, "checks": checks, "reason": "paper_session_too_old"}

    live_flag_detected = any(
        str(value).upper() == "LIVE"
        for value in session_policy.values()
        if isinstance(value, (str, bool, int, float))
    )
    checks.append(
        {
            "name": "no_live_flag_in_session_policy",
            "status": "pass" if not live_flag_detected else "fail",
            "value": live_flag_detected,
            "threshold": False,
        }
    )
    if live_flag_detected:
        return {"is_valid": False, "checks": checks, "reason": "live_flag_detected"}

    return {
        "is_valid": True,
        "checks": checks,
        "reason": "",
        "session_manifest": session_manifest,
        "session_report": session_report,
        "session_policy": session_policy,
        "paper_gate_report": paper_gate_report,
        "status": status,
        "references": refs,
    }


__all__ = ["REQUIRED_PAPER_SESSION_FILES", "validate_broker_paper_inputs"]
