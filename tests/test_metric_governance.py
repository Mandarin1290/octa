import math
import json
from pathlib import Path

from octa.core.readiness import (
    BrokerPaperReadinessPolicy,
    default_metric_governance_policy,
    evaluate_broker_paper_readiness,
    normalize_readiness_metrics,
)
from octa.core.data.recycling.common import sha256_file


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2, allow_nan=True), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _manifest_for(directory: Path, file_names: list[str]) -> dict:
    return {name: sha256_file(directory / name) for name in file_names}


def _build_chain(base: Path) -> dict[str, Path]:
    evidence_root = base / "evidence"
    export_root = base / "research_exports"
    export_dir = export_root / "research_bridge_20260320T222511Z"
    _write_text(export_dir / "signals.parquet", "signals")
    _write_text(export_dir / "returns.parquet", "returns")
    _write_json(export_dir / "metadata.json", {"strategy_name": "x", "timeframe": "1D", "params": {}, "source": "unit"})
    _write_json(
        export_dir / "export_manifest.json",
        {
            "files": {
                "signals.parquet": {"sha256": sha256_file(export_dir / "signals.parquet")},
                "returns.parquet": {"sha256": sha256_file(export_dir / "returns.parquet")},
                "metadata.json": {"sha256": sha256_file(export_dir / "metadata.json")},
            }
        },
    )

    def make_shadow(name: str) -> Path:
        d = evidence_root / name
        _write_text(d / "shadow_config.json", "{}")
        _write_text(d / "trades.parquet", "shadow-trades")
        _write_text(d / "equity_curve.parquet", "shadow-equity")
        _write_json(d / "metrics.json", {"n_trades": 2})
        _write_json(
            d / "run_manifest.json",
            {
                "hashes": _manifest_for(d, ["shadow_config.json", "trades.parquet", "equity_curve.parquet", "metrics.json"]),
                "research_export_path": str(export_dir.resolve()),
            },
        )
        return d

    def make_promotion(name: str, shadow_dir: Path) -> Path:
        d = evidence_root / name
        _write_json(d / "decision_report.json", {"status": "PROMOTE_ELIGIBLE", "shadow_evidence_dir": str(shadow_dir.resolve())})
        _write_json(d / "applied_policy.json", {"x": 1})
        _write_json(d / "evidence_manifest.json", {"hashes": _manifest_for(d, ["decision_report.json", "applied_policy.json"])})
        return d

    def make_paper_gate(name: str, promotion_dir: Path) -> Path:
        d = evidence_root / name
        _write_json(d / "paper_gate_report.json", {"gate_result": {"status": "PAPER_ELIGIBLE"}, "promotion_evidence_dir": str(promotion_dir.resolve())})
        _write_json(d / "applied_paper_policy.json", {"require_promotion_status": "PROMOTE_ELIGIBLE"})
        _write_json(d / "evidence_manifest.json", {"hashes": _manifest_for(d, ["paper_gate_report.json", "applied_paper_policy.json"])})
        return d

    def make_paper_session(name: str, paper_gate_dir: Path, promotion_dir: Path, shadow_dir: Path) -> Path:
        d = evidence_root / name
        references = {
            "paper_gate_evidence_dir": str(paper_gate_dir.resolve()),
            "promotion_evidence_dir": str(promotion_dir.resolve()),
            "shadow_evidence_dir": str(shadow_dir.resolve()),
            "research_export_path": str(export_dir.resolve()),
        }
        _write_json(
            d / "paper_session_report.json",
            {
                "blocked_reason": None,
                "references": references,
                "session_policy": {"require_gate_status": "PAPER_ELIGIBLE"},
                "session_summary": {"status": "PAPER_SESSION_COMPLETED", "symbols": ["TEST"]},
                "validation_result": {"status": "ok", "n_trades": 2, "kill_switch_triggered": False},
            },
        )
        _write_json(
            d / "session_manifest.json",
            {
                "blocked_reason": None,
                "metrics": {"final_equity": 101000.0, "kill_switch_triggered": False, "max_drawdown": 0.01, "n_trades": 2},
                "references": references,
                "session_summary": {"status": "PAPER_SESSION_COMPLETED", "symbols": ["TEST"]},
                "validation_result": {"status": "ok", "n_trades": 2},
            },
        )
        _write_json(d / "session_policy.json", {"mode": "PAPER"})
        _write_text(d / "trades.parquet", "paper-trades")
        _write_text(d / "equity_curve.parquet", "paper-equity")
        _write_json(d / "session_metrics.json", {"final_equity": 101000.0, "kill_switch_triggered": False, "max_drawdown": 0.01, "n_trades": 2})
        _write_text(d / "sample_trades.txt", "sample")
        _write_text(d / "sample_equity_head.txt", "sample")
        _write_json(d / "evidence_manifest.json", {"hashes": _manifest_for(d, ["paper_session_report.json", "session_manifest.json", "session_policy.json", "trades.parquet", "equity_curve.parquet", "session_metrics.json", "sample_trades.txt", "sample_equity_head.txt"])})
        return d

    def make_broker(name: str, paper_session_dir: Path, paper_gate_dir: Path, promotion_dir: Path, shadow_dir: Path) -> Path:
        d = evidence_root / name
        references = {
            "paper_session_evidence_dir": str(paper_session_dir.resolve()),
            "paper_gate_evidence_dir": str(paper_gate_dir.resolve()),
            "promotion_evidence_dir": str(promotion_dir.resolve()),
            "shadow_evidence_dir": str(shadow_dir.resolve()),
            "research_export_path": str(export_dir.resolve()),
        }
        _write_text(d / "orders.parquet", "orders")
        _write_text(d / "fills.parquet", "fills")
        _write_text(d / "positions.parquet", "positions")
        _write_text(d / "equity_curve.parquet", "equity")
        _write_text(d / "sample_orders.txt", "sample")
        _write_text(d / "sample_fills.txt", "sample")
        _write_text(d / "sample_equity_head.txt", "sample")
        _write_json(
            d / "metrics.json",
            {
                "final_equity": 102000.0,
                "kill_switch_triggered": False,
                "max_drawdown": 0.005,
                "n_orders": 5,
                "n_fills": 5,
                "n_trades": 2,
                "profit_factor": float("inf"),
                "total_trades": 2,
                "win_rate": 0.4,
            },
        )
        _write_json(
            d / "broker_paper_report.json",
            {
                "blocked_reason": None,
                "references": references,
                "policy": {"require_broker_mode": "PAPER", "forbid_live_mode": True},
                "gate_result": {"status": "BROKER_PAPER_ELIGIBLE", "checks": []},
                "validation_result": {"status": "ok", "n_orders": 5, "n_fills": 5, "kill_switch_triggered": False},
                "session_summary": {"status": "BROKER_PAPER_SESSION_COMPLETED", "symbols": ["TEST"]},
            },
        )
        _write_json(d / "applied_broker_paper_policy.json", {"require_broker_mode": "PAPER", "forbid_live_mode": True})
        _write_json(
            d / "evidence_manifest.json",
            {"hashes": _manifest_for(d, ["broker_paper_report.json", "applied_broker_paper_policy.json", "orders.parquet", "fills.parquet", "positions.parquet", "equity_curve.parquet", "metrics.json", "sample_orders.txt", "sample_fills.txt", "sample_equity_head.txt"])},
        )
        return d

    shadow = make_shadow("shadow_run_positive")
    promotion = make_promotion("promotion_run_positive", shadow)
    gate = make_paper_gate("paper_gate_positive", promotion)
    session = make_paper_session("paper_session_positive", gate, promotion, shadow)
    make_broker("broker_paper_positive", session, gate, promotion, shadow)
    blocked_shadow = make_shadow("shadow_run_negative")
    blocked_promotion = make_promotion("promotion_run_negative", blocked_shadow)
    blocked_gate = make_paper_gate("paper_gate_negative", blocked_promotion)
    blocked_session = evidence_root / "paper_session_negative"
    blocked_refs = {
        "paper_gate_evidence_dir": str(blocked_gate.resolve()),
        "promotion_evidence_dir": str(blocked_promotion.resolve()),
        "shadow_evidence_dir": str(blocked_shadow.resolve()),
        "research_export_path": str(export_dir.resolve()),
    }
    _write_json(blocked_session / "paper_session_report.json", {"blocked_reason": "gate_status=PAPER_BLOCKED", "references": blocked_refs, "session_policy": {"require_gate_status": "PAPER_ELIGIBLE"}, "session_summary": None, "validation_result": None})
    _write_json(blocked_session / "session_manifest.json", {"blocked_reason": "gate_status=PAPER_BLOCKED", "metrics": {}, "references": blocked_refs, "session_summary": None, "validation_result": None})
    _write_json(blocked_session / "session_policy.json", {"mode": "PAPER"})
    _write_json(blocked_session / "evidence_manifest.json", {"hashes": _manifest_for(blocked_session, ["paper_session_report.json", "session_manifest.json", "session_policy.json"])})
    blocked_broker = evidence_root / "broker_paper_negative"
    _write_json(blocked_broker / "broker_paper_report.json", {"blocked_reason": "gate_status=BROKER_PAPER_BLOCKED", "references": {"paper_session_evidence_dir": str(blocked_session.resolve()), **blocked_refs}, "policy": {"require_broker_mode": "PAPER", "forbid_live_mode": True}, "gate_result": {"status": "BROKER_PAPER_BLOCKED", "checks": []}, "validation_result": None, "session_summary": None})
    _write_json(blocked_broker / "applied_broker_paper_policy.json", {"require_broker_mode": "PAPER", "forbid_live_mode": True})
    _write_json(blocked_broker / "evidence_manifest.json", {"hashes": _manifest_for(blocked_broker, ["broker_paper_report.json", "applied_broker_paper_policy.json"])})
    return {"evidence_root": evidence_root, "research_export_root": export_root}


def _readiness_policy(metric_policy: dict) -> BrokerPaperReadinessPolicy:
    return BrokerPaperReadinessPolicy(
        require_governance_integrity=True,
        require_negative_path_proof=True,
        require_positive_path_proof=True,
        require_broker_mode_paper_only=True,
        min_completed_broker_paper_sessions=1,
        max_allowed_drawdown=0.05,
        require_kill_switch_path_tested=True,
        require_no_live_flags=True,
        require_evidence_chain_complete=True,
        metric_governance_policy=metric_policy,
    )


def test_profit_factor_inf_policy_block_not_ready(tmp_path):
    roots = _build_chain(tmp_path)
    policy = default_metric_governance_policy()
    result = evaluate_broker_paper_readiness(
        {"evidence_root": roots["evidence_root"], "research_export_root": roots["research_export_root"]},
        _readiness_policy(policy),
    )
    assert result["status"] == "BROKER_PAPER_NOT_READY"
    assert result["summary"]["metrics_summary"]["non_finite_metric_classification"] == "blocking"


def test_profit_factor_inf_cap_and_flag_with_reason_can_pass():
    policy = default_metric_governance_policy()
    policy["non_finite_metrics"]["profit_factor"]["handling"] = "cap_and_flag"
    metrics = {
        "profit_factor": float("inf"),
        "gross_loss": 0.0,
        "final_equity": 101000.0,
    }
    normalized = normalize_readiness_metrics(metrics, policy)
    assert normalized["classification"] == "normalized_with_flag"
    assert normalized["normalized"]["profit_factor"] == 999.0
    assert "profit_factor_non_finite" in normalized["flags"]
    assert "profit_factor_infinite_due_to_zero_gross_loss" in normalized["annotations"]


def test_nan_metric_blocking():
    policy = default_metric_governance_policy()
    normalized = normalize_readiness_metrics({"sharpe": float("nan")}, policy)
    assert normalized["classification"] == "blocking"
    assert normalized["normalized"]["sharpe"] is None


def test_finite_metrics_unchanged():
    policy = default_metric_governance_policy()
    metrics = {"profit_factor": 1.5, "max_drawdown": 0.02}
    normalized = normalize_readiness_metrics(metrics, policy)
    assert normalized["raw"] == metrics
    assert normalized["normalized"] == metrics
    assert normalized["flags"] == []


def test_same_inputs_same_normalized_outputs():
    policy = default_metric_governance_policy()
    policy["non_finite_metrics"]["profit_factor"]["handling"] = "cap_and_flag"
    metrics = {"profit_factor": float("inf"), "gross_loss": 0.0}
    left = normalize_readiness_metrics(metrics, policy)
    right = normalize_readiness_metrics(metrics, policy)
    assert left == right
