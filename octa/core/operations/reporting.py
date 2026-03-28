from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from octa.core.data.recycling.common import sha256_file, stable_hash


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2, default=str, allow_nan=True), encoding="utf-8")
    return path


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def write_broker_paper_ops_evidence(
    *,
    evidence_dir: str | Path,
    plan: Mapping[str, Any],
    execution: Mapping[str, Any],
    aggregated_metrics: Mapping[str, Any],
) -> dict[str, str]:
    run_dir = Path(evidence_dir)
    if run_dir.exists():
        raise FileExistsError(f"refusing to overwrite existing evidence directory: {run_dir}")
    run_dir.mkdir(parents=True, exist_ok=False)

    ops_plan = _write_json(run_dir / "ops_plan.json", dict(plan))
    ops_report = _write_json(
        run_dir / "ops_report.json",
        {
            "batch_status": execution["batch_status"],
            "runs": list(execution["runs"]),
            "summary": dict(execution["summary"]),
        },
    )
    aggregated = _write_json(run_dir / "aggregated_metrics.json", dict(aggregated_metrics))
    run_index = _write_json(
        run_dir / "run_index.json",
        {
            "executed_runs": [
                {
                    "sequence": run.get("sequence"),
                    "status": run.get("status"),
                    "evidence_dir": run.get("evidence_dir"),
                    "source_broker_paper_evidence_dir": run.get("source_broker_paper_evidence_dir"),
                }
                for run in execution["runs"]
            ]
        },
    )
    summary = _write_text(
        run_dir / "ops_summary.txt",
        "\n".join(
            [
                f"batch_status={execution['batch_status']}",
                f"n_runs_planned={len(plan.get('planned_runs', []))}",
                f"n_runs_completed={aggregated_metrics.get('n_runs_completed')}",
                f"n_runs_failed={aggregated_metrics.get('n_runs_failed')}",
                f"non_finite_metric_flags={len(aggregated_metrics.get('non_finite_metric_flags', []))}",
            ]
        )
        + "\n",
    )
    hashes = {
        "ops_plan.json": sha256_file(ops_plan),
        "ops_report.json": sha256_file(ops_report),
        "aggregated_metrics.json": sha256_file(aggregated),
        "run_index.json": sha256_file(run_index),
        "ops_summary.txt": sha256_file(summary),
    }
    manifest = _write_json(
        run_dir / "evidence_manifest.json",
        {
            "plan_hash": stable_hash(plan),
            "execution_hash": stable_hash(execution),
            "aggregated_metrics_hash": stable_hash(aggregated_metrics),
            "hashes": hashes,
        },
    )
    return {
        "ops_plan.json": str(ops_plan),
        "ops_report.json": str(ops_report),
        "aggregated_metrics.json": str(aggregated),
        "run_index.json": str(run_index),
        "ops_summary.txt": str(summary),
        "evidence_manifest.json": str(manifest),
    }


__all__ = ["write_broker_paper_ops_evidence"]
