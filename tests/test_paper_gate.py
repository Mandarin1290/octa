from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from octa.core.data.recycling.common import sha256_file
from octa.core.paper.paper_gate import evaluate_paper_gate
from octa.core.paper.paper_policy import PaperPolicy
from octa.core.paper.paper_session import start_paper_session
from octa.core.pipeline.paper_runner import run_paper_gate


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")


def _policy() -> PaperPolicy:
    return PaperPolicy(
        require_promotion_status="PROMOTE_ELIGIBLE",
        require_hash_integrity=True,
        require_recent_promotion=True,
        max_promotion_age_hours=48.0,
        require_shadow_metrics_present=True,
        paper_capital=100000.0,
        paper_fee=0.001,
        paper_slippage=0.0005,
    )


def _make_promotion_evidence(tmp_path: Path, *, status: str = "PROMOTE_ELIGIBLE") -> Path:
    promotion_dir = tmp_path / "promotion_evidence"
    shadow_dir = tmp_path / "shadow_evidence"
    promotion_dir.mkdir(parents=True)
    shadow_dir.mkdir(parents=True)
    _write_json(shadow_dir / "metrics.json", {"n_trades": 3})

    decision_report = {
        "input_evidence_dir": str(shadow_dir.resolve()),
        "policy": {"min_trades": 3},
        "decision": {
            "status": status,
            "checks": [{"name": "example", "status": "pass", "value": True, "threshold": True}],
            "summary": {
                "shadow_evidence_dir": str(shadow_dir.resolve()),
                "blocked_by_validation": False,
            },
        },
        "python_version": "3.13.5",
    }
    _write_json(promotion_dir / "decision_report.json", decision_report)
    _write_json(promotion_dir / "applied_policy.json", {"min_trades": 3})
    (promotion_dir / "promotion_summary.txt").write_text("summary\n", encoding="utf-8")
    manifest = {
        "hashes": {
            "decision_report.json": sha256_file(promotion_dir / "decision_report.json"),
            "applied_policy.json": sha256_file(promotion_dir / "applied_policy.json"),
            "promotion_summary.txt": sha256_file(promotion_dir / "promotion_summary.txt"),
        }
    }
    _write_json(promotion_dir / "evidence_manifest.json", manifest)
    return promotion_dir


def test_promote_blocked_yields_paper_blocked(tmp_path: Path) -> None:
    promotion_dir = _make_promotion_evidence(tmp_path, status="PROMOTE_BLOCKED")
    result = evaluate_paper_gate(promotion_dir, _policy())
    assert result["status"] == "PAPER_BLOCKED"


def test_promote_eligible_yields_paper_eligible(tmp_path: Path) -> None:
    promotion_dir = _make_promotion_evidence(tmp_path, status="PROMOTE_ELIGIBLE")
    result = evaluate_paper_gate(promotion_dir, _policy())
    assert result["status"] == "PAPER_ELIGIBLE"


def test_missing_promotion_artifact_blocks(tmp_path: Path) -> None:
    promotion_dir = _make_promotion_evidence(tmp_path, status="PROMOTE_ELIGIBLE")
    (promotion_dir / "decision_report.json").unlink()
    result = evaluate_paper_gate(promotion_dir, _policy())
    assert result["status"] == "PAPER_BLOCKED"
    assert result["summary"]["blocked_by_validation"] is True


def test_manipulated_hash_blocks(tmp_path: Path) -> None:
    promotion_dir = _make_promotion_evidence(tmp_path, status="PROMOTE_ELIGIBLE")
    (promotion_dir / "promotion_summary.txt").write_text("tampered\n", encoding="utf-8")
    result = evaluate_paper_gate(promotion_dir, _policy())
    assert result["status"] == "PAPER_BLOCKED"
    assert result["summary"]["blocked_by_validation"] is True


def test_deterministic_same_inputs_same_output(tmp_path: Path) -> None:
    promotion_dir = _make_promotion_evidence(tmp_path, status="PROMOTE_ELIGIBLE")
    first = evaluate_paper_gate(promotion_dir, _policy())
    second = evaluate_paper_gate(promotion_dir, _policy())
    assert first == second


def test_start_paper_session_only_when_eligible(tmp_path: Path) -> None:
    promotion_dir = _make_promotion_evidence(tmp_path, status="PROMOTE_ELIGIBLE")
    gate_result = evaluate_paper_gate(promotion_dir, _policy())
    session = start_paper_session(gate_result, _policy().to_dict())
    assert session["status"] == "PAPER_SESSION_STARTED"

    blocked = evaluate_paper_gate(_make_promotion_evidence(tmp_path / "blocked", status="PROMOTE_BLOCKED"), _policy())
    try:
        start_paper_session(blocked, _policy().to_dict())
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for blocked paper session start")


def test_runner_writes_blocked_reports(tmp_path: Path) -> None:
    promotion_dir = _make_promotion_evidence(tmp_path, status="PROMOTE_BLOCKED")
    result = run_paper_gate(
        promotion_evidence_dir=promotion_dir,
        policy=_policy(),
        evidence_root=tmp_path / "evidence",
        run_id="paper_gate_fixture",
        start_session=False,
    )
    assert result["status"] == "PAPER_BLOCKED"
    assert (tmp_path / "evidence" / "paper_gate_fixture" / "paper_gate_report.json").exists()
