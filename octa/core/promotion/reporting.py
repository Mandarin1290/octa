from __future__ import annotations

import json
import platform
from pathlib import Path
from typing import Any

from octa.core.data.recycling.common import sha256_file, stable_hash


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2, default=str), encoding="utf-8")
    return path


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def write_promotion_reports(
    *,
    evidence_dir: str | Path,
    input_evidence_dir: str | Path,
    policy: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> dict[str, str]:
    run_dir = Path(evidence_dir)
    if run_dir.exists():
        raise FileExistsError(f"refusing to overwrite existing evidence directory: {run_dir}")
    run_dir.mkdir(parents=True, exist_ok=False)

    decision_payload = {
        "input_evidence_dir": str(Path(input_evidence_dir).resolve()),
        "policy": dict(policy),
        "decision": dict(decision),
        "python_version": platform.python_version(),
    }
    decision_path = _write_json(run_dir / "decision_report.json", decision_payload)
    policy_path = _write_json(run_dir / "applied_policy.json", dict(policy))

    summary_lines = [
        f"input_evidence_dir={Path(input_evidence_dir).resolve()}",
        f"status={decision['status']}",
        f"checks_total={len(decision['checks'])}",
        f"checks_failed={sum(1 for item in decision['checks'] if item['status'] == 'fail')}",
    ]
    summary_path = _write_text(run_dir / "promotion_summary.txt", "\n".join(summary_lines) + "\n")

    manifest_payload = {
        "input_evidence_dir": str(Path(input_evidence_dir).resolve()),
        "policy_hash": stable_hash(policy),
        "decision_hash": stable_hash(decision),
        "hashes": {
            "decision_report.json": sha256_file(decision_path),
            "promotion_summary.txt": sha256_file(summary_path),
            "applied_policy.json": sha256_file(policy_path),
        },
    }
    manifest_path = _write_json(run_dir / "evidence_manifest.json", manifest_payload)
    return {
        "decision_report.json": str(decision_path),
        "promotion_summary.txt": str(summary_path),
        "applied_policy.json": str(policy_path),
        "evidence_manifest.json": str(manifest_path),
    }


__all__ = ["write_promotion_reports"]
