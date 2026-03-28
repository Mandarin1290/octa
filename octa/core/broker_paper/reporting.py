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


def write_broker_paper_evidence(
    *,
    evidence_dir: str | Path,
    references: Mapping[str, str],
    policy: Mapping[str, Any],
    gate_result: Mapping[str, Any],
    session_result: Mapping[str, Any] | None,
    validation_result: Mapping[str, Any] | None,
    blocked_reason: str | None = None,
) -> dict[str, str]:
    run_dir = Path(evidence_dir)
    if run_dir.exists():
        raise FileExistsError(f"refusing to overwrite existing evidence directory: {run_dir}")
    run_dir.mkdir(parents=True, exist_ok=False)

    report_path = _write_json(
        run_dir / "broker_paper_report.json",
        {
            "references": dict(references),
            "policy": dict(policy),
            "gate_result": dict(gate_result),
            "blocked_reason": blocked_reason,
            "validation_result": dict(validation_result) if validation_result is not None else None,
            "session_summary": dict(session_result["summary"]) if session_result is not None else None,
        },
    )
    summary_lines = [
        f"paper_session_evidence_dir={references['paper_session_evidence_dir']}",
        f"status={session_result['session_status'] if session_result is not None else gate_result['status']}",
        f"blocked_reason={blocked_reason}",
    ]
    summary_path = _write_text(run_dir / "broker_paper_summary.txt", "\n".join(summary_lines) + "\n")
    policy_path = _write_json(run_dir / "applied_broker_paper_policy.json", dict(policy))
    hashes = {
        "broker_paper_report.json": sha256_file(report_path),
        "broker_paper_summary.txt": sha256_file(summary_path),
        "applied_broker_paper_policy.json": sha256_file(policy_path),
    }

    if session_result is not None:
        orders_path = run_dir / "orders.parquet"
        fills_path = run_dir / "fills.parquet"
        positions_path = run_dir / "positions.parquet"
        equity_path = run_dir / "equity_curve.parquet"
        metrics_path = run_dir / "metrics.json"
        sample_orders_path = run_dir / "sample_orders.txt"
        sample_fills_path = run_dir / "sample_fills.txt"
        sample_equity_path = run_dir / "sample_equity_head.txt"
        session_result["orders"].to_parquet(orders_path)
        session_result["fills"].to_parquet(fills_path)
        session_result["positions"].to_parquet(positions_path)
        session_result["equity_curve"].to_parquet(equity_path)
        _write_json(metrics_path, dict(session_result["metrics"]))
        _write_text(sample_orders_path, session_result["orders"].head().to_string() + "\n")
        _write_text(sample_fills_path, session_result["fills"].head().to_string() + "\n")
        _write_text(sample_equity_path, session_result["equity_curve"].head().to_string() + "\n")
        hashes.update(
            {
                "orders.parquet": sha256_file(orders_path),
                "fills.parquet": sha256_file(fills_path),
                "positions.parquet": sha256_file(positions_path),
                "equity_curve.parquet": sha256_file(equity_path),
                "metrics.json": sha256_file(metrics_path),
                "sample_orders.txt": sha256_file(sample_orders_path),
                "sample_fills.txt": sha256_file(sample_fills_path),
                "sample_equity_head.txt": sha256_file(sample_equity_path),
            }
        )

    manifest_path = _write_json(
        run_dir / "evidence_manifest.json",
        {
            "references": dict(references),
            "policy_hash": stable_hash(policy),
            "gate_result_hash": stable_hash(gate_result),
            "hashes": hashes,
        },
    )
    return {
        "broker_paper_report.json": str(report_path),
        "broker_paper_summary.txt": str(summary_path),
        "applied_broker_paper_policy.json": str(policy_path),
        "evidence_manifest.json": str(manifest_path),
    }


__all__ = ["write_broker_paper_evidence"]
