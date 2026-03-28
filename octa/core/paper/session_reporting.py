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


def write_paper_session_evidence(
    *,
    evidence_dir: str | Path,
    references: Mapping[str, str],
    session_policy: Mapping[str, Any],
    session_result: Mapping[str, Any] | None,
    validation_result: Mapping[str, Any] | None,
    blocked_reason: str | None = None,
) -> dict[str, str]:
    run_dir = Path(evidence_dir)
    if run_dir.exists():
        raise FileExistsError(f"refusing to overwrite existing evidence directory: {run_dir}")
    run_dir.mkdir(parents=True, exist_ok=False)

    session_manifest = {
        "references": dict(references),
        "blocked_reason": blocked_reason,
        "validation_result": dict(validation_result) if validation_result is not None else None,
    }
    if session_result is not None:
        session_manifest["session_summary"] = dict(session_result["session_summary"])
        session_manifest["metrics"] = dict(session_result["metrics"])
    session_manifest_path = _write_json(run_dir / "session_manifest.json", session_manifest)

    report_payload = {
        "references": dict(references),
        "session_policy": dict(session_policy),
        "blocked_reason": blocked_reason,
        "validation_result": dict(validation_result) if validation_result is not None else None,
        "session_summary": dict(session_result["session_summary"]) if session_result is not None else None,
    }
    report_path = _write_json(run_dir / "paper_session_report.json", report_payload)
    policy_path = _write_json(run_dir / "session_policy.json", dict(session_policy))

    hashes = {
        "session_manifest.json": sha256_file(session_manifest_path),
        "paper_session_report.json": sha256_file(report_path),
        "session_policy.json": sha256_file(policy_path),
    }

    if session_result is not None:
        trades_path = run_dir / "trades.parquet"
        equity_path = run_dir / "equity_curve.parquet"
        metrics_path = run_dir / "session_metrics.json"
        sample_trades_path = run_dir / "sample_trades.txt"
        sample_equity_path = run_dir / "sample_equity_head.txt"
        session_result["trades"].to_parquet(trades_path)
        session_result["equity_curve"].to_parquet(equity_path)
        _write_json(metrics_path, dict(session_result["metrics"]))
        _write_text(sample_trades_path, session_result["trades"].head().to_string() + "\n")
        _write_text(sample_equity_path, session_result["equity_curve"].head().to_string() + "\n")
        hashes.update(
            {
                "trades.parquet": sha256_file(trades_path),
                "equity_curve.parquet": sha256_file(equity_path),
                "session_metrics.json": sha256_file(metrics_path),
                "sample_trades.txt": sha256_file(sample_trades_path),
                "sample_equity_head.txt": sha256_file(sample_equity_path),
            }
        )

    evidence_manifest_path = _write_json(
        run_dir / "evidence_manifest.json",
        {
            "references": dict(references),
            "session_policy_hash": stable_hash(session_policy),
            "session_result_hash": stable_hash(report_payload),
            "hashes": hashes,
        },
    )
    return {
        "session_manifest.json": str(session_manifest_path),
        "paper_session_report.json": str(report_path),
        "session_policy.json": str(policy_path),
        "evidence_manifest.json": str(evidence_manifest_path),
    }


__all__ = ["write_paper_session_evidence"]
