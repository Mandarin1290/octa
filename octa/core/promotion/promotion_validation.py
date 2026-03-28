from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from octa.core.data.recycling.common import sha256_file


REQUIRED_FILES = ("metrics.json", "run_manifest.json", "shadow_config.json")
REQUIRED_METRICS = (
    "total_return",
    "sharpe",
    "max_drawdown",
    "win_rate",
    "profit_factor",
    "n_trades",
    "kill_switch_triggered",
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


def validate_shadow_evidence(
    shadow_evidence_dir: str | Path,
    *,
    require_hash_integrity: bool,
    require_validation_ok: bool,
) -> dict[str, Any]:
    evidence_dir = Path(shadow_evidence_dir)
    checks: list[dict[str, Any]] = []

    if not evidence_dir.exists() or not evidence_dir.is_dir():
        return {
            "is_valid": False,
            "checks": [{
                "name": "evidence_dir_exists",
                "status": "fail",
                "value": str(evidence_dir),
                "threshold": "existing_directory",
            }],
            "reason": "missing_evidence_dir",
        }

    for name in REQUIRED_FILES:
        path = evidence_dir / name
        exists = path.exists()
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

    manifest = _load_json(evidence_dir / "run_manifest.json")
    metrics = _load_json(evidence_dir / "metrics.json")
    shadow_config = _load_json(evidence_dir / "shadow_config.json")
    validation_report = None
    validation_path = evidence_dir / "validation_report.json"
    if validation_path.exists():
        validation_report = _load_json(validation_path)

    hashes = manifest.get("hashes", {})
    if not isinstance(hashes, dict):
        return {"is_valid": False, "checks": checks, "reason": "invalid_run_manifest_hashes"}

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

    missing_metrics = [name for name in REQUIRED_METRICS if name not in metrics]
    checks.append(
        {
            "name": "metrics_complete",
            "status": "pass" if not missing_metrics else "fail",
            "value": missing_metrics,
            "threshold": [],
        }
    )
    if missing_metrics:
        return {"is_valid": False, "checks": checks, "reason": f"missing_metrics:{','.join(missing_metrics)}"}

    for name in REQUIRED_METRICS:
        value = metrics[name]
        if isinstance(value, bool):
            continue
        if not isinstance(value, (int, float)) or math.isnan(float(value)):
            checks.append(
                {
                    "name": f"metric_valid:{name}",
                    "status": "fail",
                    "value": value,
                    "threshold": "finite_numeric",
                }
            )
            return {"is_valid": False, "checks": checks, "reason": f"invalid_metric:{name}"}
        checks.append(
            {
                "name": f"metric_valid:{name}",
                "status": "pass",
                "value": value,
                "threshold": "finite_numeric",
            }
        )

    if require_validation_ok:
        research_status = manifest.get("research_validation", {}).get("status")
        shadow_status = manifest.get("shadow_validation", {}).get("status")
        checks.append(
            {
                "name": "research_validation_ok",
                "status": "pass" if research_status == "ok" else "fail",
                "value": research_status,
                "threshold": "ok",
            }
        )
        checks.append(
            {
                "name": "shadow_validation_ok",
                "status": "pass" if shadow_status == "ok" else "fail",
                "value": shadow_status,
                "threshold": "ok",
            }
        )
        if validation_report is not None:
            local_status = validation_report.get("status")
            checks.append(
                {
                    "name": "optional_validation_report_ok",
                    "status": "pass" if local_status == "ok" else "fail",
                    "value": local_status,
                    "threshold": "ok",
                }
            )
            if local_status != "ok":
                return {"is_valid": False, "checks": checks, "reason": "optional_validation_report_failed"}
        if research_status != "ok" or shadow_status != "ok":
            return {"is_valid": False, "checks": checks, "reason": "manifest_validation_not_ok"}

    return {
        "is_valid": True,
        "checks": checks,
        "reason": "",
        "metrics": metrics,
        "manifest": manifest,
        "shadow_config": shadow_config,
        "validation_report": validation_report,
    }


__all__ = ["REQUIRED_FILES", "REQUIRED_METRICS", "validate_shadow_evidence"]
