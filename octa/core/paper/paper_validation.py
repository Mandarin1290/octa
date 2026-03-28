from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from octa.core.data.recycling.common import sha256_file


REQUIRED_PROMOTION_FILES = (
    "decision_report.json",
    "applied_policy.json",
    "evidence_manifest.json",
)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON in {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def validate_promotion_evidence(
    promotion_evidence_dir: str | Path,
    *,
    require_hash_integrity: bool,
    require_recent_promotion: bool,
    max_promotion_age_hours: float,
    require_shadow_metrics_present: bool,
) -> dict[str, Any]:
    evidence_dir = Path(promotion_evidence_dir)
    checks: list[dict[str, Any]] = []

    if not evidence_dir.exists() or not evidence_dir.is_dir():
        return {
            "is_valid": False,
            "checks": [{
                "name": "promotion_evidence_dir_exists",
                "status": "fail",
                "value": str(evidence_dir),
                "threshold": "existing_directory",
            }],
            "reason": "missing_promotion_evidence_dir",
        }

    for name in REQUIRED_PROMOTION_FILES:
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

    decision_report = _load_json(evidence_dir / "decision_report.json")
    applied_policy = _load_json(evidence_dir / "applied_policy.json")
    evidence_manifest = _load_json(evidence_dir / "evidence_manifest.json")

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

    decision = decision_report.get("decision")
    if not isinstance(decision, dict):
        return {"is_valid": False, "checks": checks, "reason": "missing_decision_payload"}

    promotion_status = decision.get("status")
    checks.append(
        {
            "name": "promotion_status_present",
            "status": "pass" if isinstance(promotion_status, str) else "fail",
            "value": promotion_status,
            "threshold": "PROMOTE_BLOCKED|PROMOTE_ELIGIBLE",
        }
    )
    if promotion_status not in {"PROMOTE_BLOCKED", "PROMOTE_ELIGIBLE"}:
        return {"is_valid": False, "checks": checks, "reason": "invalid_promotion_status"}

    shadow_evidence_dir = decision.get("summary", {}).get("shadow_evidence_dir")
    if not isinstance(shadow_evidence_dir, str):
        return {"is_valid": False, "checks": checks, "reason": "missing_shadow_evidence_dir"}
    shadow_path = Path(shadow_evidence_dir)
    checks.append(
        {
            "name": "shadow_evidence_dir_exists",
            "status": "pass" if shadow_path.exists() else "fail",
            "value": str(shadow_path),
            "threshold": "existing_directory",
        }
    )
    if not shadow_path.exists():
        return {"is_valid": False, "checks": checks, "reason": "missing_shadow_evidence_dir"}

    if require_shadow_metrics_present:
        metrics_exists = (shadow_path / "metrics.json").exists()
        checks.append(
            {
                "name": "shadow_metrics_present",
                "status": "pass" if metrics_exists else "fail",
                "value": metrics_exists,
                "threshold": True,
            }
        )
        if not metrics_exists:
            return {"is_valid": False, "checks": checks, "reason": "missing_shadow_metrics"}

    if require_recent_promotion:
        age_seconds = int(
            datetime.now(timezone.utc).timestamp()
            - (evidence_dir / "decision_report.json").stat().st_mtime
        )
        age_hours = age_seconds / 3600.0
        checks.append(
            {
                "name": "promotion_age_hours",
                "status": "pass" if age_hours <= max_promotion_age_hours else "fail",
                "value": age_hours,
                "threshold": max_promotion_age_hours,
            }
        )
        if age_hours > max_promotion_age_hours:
            return {"is_valid": False, "checks": checks, "reason": "promotion_too_old"}

    return {
        "is_valid": True,
        "checks": checks,
        "reason": "",
        "decision_report": decision_report,
        "applied_policy": applied_policy,
        "evidence_manifest": evidence_manifest,
        "shadow_evidence_dir": str(shadow_path.resolve()),
    }


__all__ = ["REQUIRED_PROMOTION_FILES", "validate_promotion_evidence"]
