from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from octa.core.data.recycling.common import sha256_file, stable_hash


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2, default=str), encoding="utf-8")
    return path


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def write_paper_reports(
    *,
    evidence_dir: str | Path,
    promotion_evidence_dir: str | Path,
    policy: Mapping[str, Any],
    gate_result: Mapping[str, Any],
    session_manifest: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    run_dir = Path(evidence_dir)
    if run_dir.exists():
        raise FileExistsError(f"refusing to overwrite existing evidence directory: {run_dir}")
    run_dir.mkdir(parents=True, exist_ok=False)

    gate_report_path = _write_json(
        run_dir / "paper_gate_report.json",
        {
            "promotion_evidence_dir": str(Path(promotion_evidence_dir).resolve()),
            "policy": dict(policy),
            "gate_result": dict(gate_result),
        },
    )
    summary_lines = [
        f"promotion_evidence_dir={Path(promotion_evidence_dir).resolve()}",
        f"status={gate_result['status']}",
        f"checks_total={len(gate_result['checks'])}",
        f"checks_failed={sum(1 for item in gate_result['checks'] if item['status'] == 'fail')}",
    ]
    summary_path = _write_text(run_dir / "paper_gate_summary.txt", "\n".join(summary_lines) + "\n")
    policy_path = _write_json(run_dir / "applied_paper_policy.json", dict(policy))

    hashes = {
        "paper_gate_report.json": sha256_file(gate_report_path),
        "paper_gate_summary.txt": sha256_file(summary_path),
        "applied_paper_policy.json": sha256_file(policy_path),
    }
    session_path = None
    if session_manifest is not None:
        session_path = _write_json(run_dir / "session_manifest.json", dict(session_manifest))
        hashes["session_manifest.json"] = sha256_file(session_path)

    manifest_path = _write_json(
        run_dir / "evidence_manifest.json",
        {
            "promotion_evidence_dir": str(Path(promotion_evidence_dir).resolve()),
            "policy_hash": stable_hash(policy),
            "gate_result_hash": stable_hash(gate_result),
            "hashes": hashes,
        },
    )
    return {
        "paper_gate_report.json": str(gate_report_path),
        "paper_gate_summary.txt": str(summary_path),
        "applied_paper_policy.json": str(policy_path),
        "evidence_manifest.json": str(manifest_path),
        **({"session_manifest.json": str(session_path)} if session_path is not None else {}),
    }


__all__ = ["write_paper_reports"]
