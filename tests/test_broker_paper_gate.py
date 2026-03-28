from __future__ import annotations

import json
from pathlib import Path

from octa.core.broker_paper.broker_paper_gate import evaluate_broker_paper_gate
from octa.core.broker_paper.broker_paper_policy import BrokerPaperPolicy
from octa.core.data.recycling.common import sha256_file


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")


def _policy() -> BrokerPaperPolicy:
    return BrokerPaperPolicy(
        require_paper_gate_status="PAPER_ELIGIBLE",
        require_min_completed_sessions=1,
        require_min_total_trades=1,
        require_min_win_rate=0.5,
        require_min_profit_factor=0.5,
        max_allowed_drawdown=0.2,
        require_kill_switch_not_triggered=True,
        require_hash_integrity=True,
        require_broker_mode="PAPER",
        forbid_live_mode=True,
        max_session_age_hours=48.0,
        paper_capital=100000.0,
        paper_fee=0.0005,
        paper_slippage=0.0002,
        max_open_positions=1,
        kill_switch_drawdown=0.2,
        allow_short=False,
    )


def _make_paper_session_evidence(tmp_path: Path, *, session_status: str = "PAPER_SESSION_COMPLETED", gate_status: str = "PAPER_ELIGIBLE") -> Path:
    session_dir = tmp_path / "paper_session"
    gate_dir = tmp_path / "paper_gate"
    promotion_dir = tmp_path / "promotion"
    shadow_dir = tmp_path / "shadow"
    research_dir = tmp_path / "research"
    for path in (session_dir, gate_dir, promotion_dir, shadow_dir, research_dir):
        path.mkdir(parents=True, exist_ok=True)
    (research_dir / "signals.parquet").write_text("stub", encoding="utf-8")
    (shadow_dir / "metrics.json").write_text("{}", encoding="utf-8")
    (promotion_dir / "decision_report.json").write_text(json.dumps({"decision": {"status": "PROMOTE_ELIGIBLE"}}), encoding="utf-8")
    (gate_dir / "paper_gate_report.json").write_text(json.dumps({"gate_result": {"status": gate_status}}), encoding="utf-8")
    _write_json(
        session_dir / "session_manifest.json",
        {
            "references": {
                "paper_gate_evidence_dir": str(gate_dir.resolve()),
                "promotion_evidence_dir": str(promotion_dir.resolve()),
                "shadow_evidence_dir": str(shadow_dir.resolve()),
                "research_export_path": str(research_dir.resolve()),
            },
            "metrics": {
                "n_trades": 2,
                "win_rate": 1.0,
                "profit_factor": 2.0,
                "max_drawdown": 0.01,
                "kill_switch_triggered": False,
            },
        },
    )
    _write_json(
        session_dir / "paper_session_report.json",
        {
            "references": {
                "paper_gate_evidence_dir": str(gate_dir.resolve()),
                "promotion_evidence_dir": str(promotion_dir.resolve()),
                "shadow_evidence_dir": str(shadow_dir.resolve()),
                "research_export_path": str(research_dir.resolve()),
            },
            "blocked_reason": None if session_status != "PAPER_BLOCKED" else "gate_status=PAPER_BLOCKED",
            "session_summary": None if session_status == "PAPER_BLOCKED" else {"status": session_status},
        },
    )
    _write_json(session_dir / "session_policy.json", {"paper_fee": 0.0005})
    manifest = {
        "hashes": {
            "session_manifest.json": sha256_file(session_dir / "session_manifest.json"),
            "paper_session_report.json": sha256_file(session_dir / "paper_session_report.json"),
            "session_policy.json": sha256_file(session_dir / "session_policy.json"),
        }
    }
    _write_json(session_dir / "evidence_manifest.json", manifest)
    return session_dir


def test_completed_session_with_good_policy_is_eligible(tmp_path: Path) -> None:
    session_dir = _make_paper_session_evidence(tmp_path)
    result = evaluate_broker_paper_gate(session_dir, _policy())
    assert result["status"] == "BROKER_PAPER_ELIGIBLE"


def test_blocked_or_missing_artifacts_block(tmp_path: Path) -> None:
    session_dir = _make_paper_session_evidence(tmp_path, session_status="PAPER_BLOCKED", gate_status="PAPER_BLOCKED")
    result = evaluate_broker_paper_gate(session_dir, _policy())
    assert result["status"] == "BROKER_PAPER_BLOCKED"
    (session_dir / "paper_session_report.json").unlink()
    blocked = evaluate_broker_paper_gate(session_dir, _policy())
    assert blocked["status"] == "BROKER_PAPER_BLOCKED"


def test_manipulated_hash_blocks(tmp_path: Path) -> None:
    session_dir = _make_paper_session_evidence(tmp_path)
    (session_dir / "session_policy.json").write_text("tampered", encoding="utf-8")
    result = evaluate_broker_paper_gate(session_dir, _policy())
    assert result["status"] == "BROKER_PAPER_BLOCKED"


def test_deterministic_same_inputs_same_output(tmp_path: Path) -> None:
    session_dir = _make_paper_session_evidence(tmp_path)
    first = evaluate_broker_paper_gate(session_dir, _policy())
    second = evaluate_broker_paper_gate(session_dir, _policy())
    assert first == second
