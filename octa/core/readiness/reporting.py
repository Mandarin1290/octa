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


def write_broker_paper_readiness_evidence(
    *,
    evidence_dir: str | Path,
    result: Mapping[str, Any],
    artifact_suffix: str = "",
) -> dict[str, str]:
    run_dir = Path(evidence_dir)
    if run_dir.exists():
        raise FileExistsError(f"refusing to overwrite existing evidence directory: {run_dir}")
    run_dir.mkdir(parents=True, exist_ok=False)

    suffix = f"_{artifact_suffix}" if artifact_suffix else ""

    inventory_name = f"readiness_inventory{suffix}.json"
    governance_name = f"readiness_governance_report{suffix}.json"
    metrics_name = f"readiness_metrics_report{suffix}.json"
    report_name = f"readiness_report{suffix}.json"
    summary_name = f"readiness_summary{suffix}.txt"
    policy_name = f"applied_readiness_policy{suffix}.json"

    inventory_path = _write_json(run_dir / inventory_name, result["inventory"])
    governance_path = _write_json(run_dir / governance_name, result["governance_report"])
    metrics_path = _write_json(run_dir / metrics_name, result["metrics_report"])
    report_path = _write_json(
        run_dir / report_name,
        {
            "status": result["status"],
            "checks": list(result["checks"]),
            "summary": dict(result["summary"]),
        },
    )
    policy_path = _write_json(run_dir / policy_name, result["summary"]["policy"])
    summary_lines = [
        f"status={result['status']}",
        f"completed_broker_paper_sessions={result['summary']['metrics_summary']['completed_broker_paper_sessions']}",
        f"blocked_broker_paper_runs={result['summary']['metrics_summary']['blocked_broker_paper_runs']}",
        f"max_observed_drawdown={result['summary']['metrics_summary']['max_observed_drawdown']}",
        f"non_finite_metric_classification={result['summary']['metrics_summary']['non_finite_metric_classification']}",
        f"policy_decision_reason={result['summary']['metrics_summary']['policy_decision_reason']}",
    ]
    summary_lines.extend(f"risk={risk}" for risk in result["summary"]["risks"])
    summary_path = _write_text(run_dir / summary_name, "\n".join(summary_lines) + "\n")
    hashes = {
        inventory_name: sha256_file(inventory_path),
        governance_name: sha256_file(governance_path),
        metrics_name: sha256_file(metrics_path),
        report_name: sha256_file(report_path),
        policy_name: sha256_file(policy_path),
        summary_name: sha256_file(summary_path),
    }
    manifest_path = _write_json(
        run_dir / "evidence_manifest.json",
        {
            "status": result["status"],
            "policy_hash": stable_hash(result["summary"]["policy"]),
            "result_hash": stable_hash(
                {
                    "status": result["status"],
                    "checks": result["checks"],
                    "summary": result["summary"],
                }
            ),
            "hashes": hashes,
        },
    )
    return {
        inventory_name: str(inventory_path),
        governance_name: str(governance_path),
        metrics_name: str(metrics_path),
        report_name: str(report_path),
        summary_name: str(summary_path),
        policy_name: str(policy_path),
        "evidence_manifest.json": str(manifest_path),
    }


__all__ = ["write_broker_paper_readiness_evidence"]
