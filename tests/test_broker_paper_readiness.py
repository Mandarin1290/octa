import json
from pathlib import Path

from octa.core.data.recycling.common import sha256_file
from octa.core.readiness.broker_paper_readiness_engine import evaluate_broker_paper_readiness
from octa.core.readiness.broker_paper_readiness_policy import BrokerPaperReadinessPolicy


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2, allow_nan=True), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _manifest_for(directory: Path, file_names: list[str]) -> dict:
    return {name: sha256_file(directory / name) for name in file_names}


def _build_chain(base: Path, *, include_negative: bool = True, include_positive: bool = True, live_flag: bool = False) -> dict[str, Path]:
    evidence_root = base / "evidence"
    export_root = base / "research_exports"
    export_dir = export_root / "research_bridge_20260320T222511Z"
    _write_text(export_dir / "signals.parquet", "signals")
    _write_text(export_dir / "returns.parquet", "returns")
    _write_json(export_dir / "metadata.json", {"strategy_name": "x", "timeframe": "1D", "params": {}, "source": "unit"})
    export_manifest = {
        "files": {
            "signals.parquet": {"sha256": sha256_file(export_dir / "signals.parquet")},
            "returns.parquet": {"sha256": sha256_file(export_dir / "returns.parquet")},
            "metadata.json": {"sha256": sha256_file(export_dir / "metadata.json")},
        }
    }
    _write_json(export_dir / "export_manifest.json", export_manifest)

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

    def make_paper_session(name: str, paper_gate_dir: Path, promotion_dir: Path, shadow_dir: Path, *, blocked: bool) -> Path:
        d = evidence_root / name
        references = {
            "paper_gate_evidence_dir": str(paper_gate_dir.resolve()),
            "promotion_evidence_dir": str(promotion_dir.resolve()),
            "shadow_evidence_dir": str(shadow_dir.resolve()),
            "research_export_path": str(export_dir.resolve()),
        }
        if blocked:
            report = {
                "blocked_reason": "gate_status=PAPER_BLOCKED",
                "references": references,
                "session_policy": {"require_gate_status": "PAPER_ELIGIBLE"},
                "session_summary": None,
                "validation_result": None,
            }
            manifest = {
                "blocked_reason": "gate_status=PAPER_BLOCKED",
                "metrics": {},
                "references": references,
                "session_summary": None,
                "validation_result": None,
            }
        else:
            report = {
                "blocked_reason": None,
                "references": references,
                "session_policy": {"require_gate_status": "PAPER_ELIGIBLE"},
                "session_summary": {"status": "PAPER_SESSION_COMPLETED", "symbols": ["TEST"]},
                "validation_result": {"status": "ok", "n_trades": 2, "kill_switch_triggered": False},
            }
            manifest = {
                "blocked_reason": None,
                "metrics": {
                    "final_equity": 101000.0,
                    "kill_switch_triggered": False,
                    "max_drawdown": 0.01,
                    "n_trades": 2,
                },
                "references": references,
                "session_summary": {"status": "PAPER_SESSION_COMPLETED", "symbols": ["TEST"]},
                "validation_result": {"status": "ok", "n_trades": 2, "kill_switch_triggered": False},
            }
            _write_text(d / "trades.parquet", "paper-trades")
            _write_text(d / "equity_curve.parquet", "paper-equity")
            _write_json(d / "session_metrics.json", manifest["metrics"])
            _write_text(d / "sample_trades.txt", "sample")
            _write_text(d / "sample_equity_head.txt", "sample")
        _write_json(d / "paper_session_report.json", report)
        _write_json(d / "session_manifest.json", manifest)
        _write_json(d / "session_policy.json", {"mode": "PAPER" if not live_flag else "LIVE"})
        file_names = ["paper_session_report.json", "session_manifest.json", "session_policy.json"]
        if not blocked:
            file_names.extend(["trades.parquet", "equity_curve.parquet", "session_metrics.json", "sample_trades.txt", "sample_equity_head.txt"])
        _write_json(d / "evidence_manifest.json", {"hashes": _manifest_for(d, file_names)})
        return d

    def make_broker(name: str, paper_session_dir: Path, paper_gate_dir: Path, promotion_dir: Path, shadow_dir: Path, *, blocked: bool) -> Path:
        d = evidence_root / name
        references = {
            "paper_session_evidence_dir": str(paper_session_dir.resolve()),
            "paper_gate_evidence_dir": str(paper_gate_dir.resolve()),
            "promotion_evidence_dir": str(promotion_dir.resolve()),
            "shadow_evidence_dir": str(shadow_dir.resolve()),
            "research_export_path": str(export_dir.resolve()),
        }
        policy = {
            "require_broker_mode": "LIVE" if live_flag else "PAPER",
            "forbid_live_mode": True,
        }
        if blocked:
            report = {
                "blocked_reason": "gate_status=BROKER_PAPER_BLOCKED",
                "references": references,
                "policy": policy,
                "gate_result": {"status": "BROKER_PAPER_BLOCKED", "checks": []},
                "validation_result": None,
                "session_summary": None,
            }
            _write_json(d / "broker_paper_report.json", report)
            _write_json(d / "applied_broker_paper_policy.json", policy)
            _write_json(d / "evidence_manifest.json", {"hashes": _manifest_for(d, ["broker_paper_report.json", "applied_broker_paper_policy.json"])})
        else:
            report = {
                "blocked_reason": None,
                "references": references,
                "policy": policy,
                "gate_result": {"status": "BROKER_PAPER_ELIGIBLE", "checks": []},
                "validation_result": {"status": "ok", "n_orders": 5, "n_fills": 5, "kill_switch_triggered": False},
                "session_summary": {"status": "BROKER_PAPER_SESSION_COMPLETED", "symbols": ["TEST"]},
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
                    "win_rate": 0.6,
                    "profit_factor": 1.5,
                },
            )
            _write_json(d / "broker_paper_report.json", report)
            _write_json(d / "applied_broker_paper_policy.json", policy)
            _write_json(
                d / "evidence_manifest.json",
                {
                    "hashes": _manifest_for(
                        d,
                        [
                            "broker_paper_report.json",
                            "applied_broker_paper_policy.json",
                            "orders.parquet",
                            "fills.parquet",
                            "positions.parquet",
                            "equity_curve.parquet",
                            "metrics.json",
                            "sample_orders.txt",
                            "sample_fills.txt",
                            "sample_equity_head.txt",
                        ],
                    )
                },
            )
        return d

    created: dict[str, Path] = {"evidence_root": evidence_root, "research_export_root": export_root}
    if include_negative:
        shadow = make_shadow("shadow_run_negative")
        promotion = make_promotion("promotion_run_negative", shadow)
        gate = make_paper_gate("paper_gate_negative", promotion)
        session = make_paper_session("paper_session_negative", gate, promotion, shadow, blocked=True)
        broker = make_broker("broker_paper_negative", session, gate, promotion, shadow, blocked=True)
        created["negative"] = broker
    if include_positive:
        shadow = make_shadow("shadow_run_positive")
        promotion = make_promotion("promotion_run_positive", shadow)
        gate = make_paper_gate("paper_gate_positive", promotion)
        session = make_paper_session("paper_session_positive", gate, promotion, shadow, blocked=False)
        broker = make_broker("broker_paper_positive", session, gate, promotion, shadow, blocked=False)
        created["positive"] = broker
    return created


def _policy(min_completed: int = 1) -> BrokerPaperReadinessPolicy:
    return BrokerPaperReadinessPolicy(
        require_governance_integrity=True,
        require_negative_path_proof=True,
        require_positive_path_proof=True,
        require_broker_mode_paper_only=True,
        min_completed_broker_paper_sessions=min_completed,
        max_allowed_drawdown=0.05,
        require_kill_switch_path_tested=True,
        require_no_live_flags=True,
        require_evidence_chain_complete=True,
    )


def test_missing_governance_artifacts_not_ready(tmp_path):
    roots = _build_chain(tmp_path)
    (roots["negative"] / "evidence_manifest.json").unlink()
    result = evaluate_broker_paper_readiness(
        {"evidence_root": roots["evidence_root"], "research_export_root": roots["research_export_root"]},
        _policy(),
    )
    assert result["status"] == "BROKER_PAPER_NOT_READY"


def test_no_positive_path_not_ready(tmp_path):
    roots = _build_chain(tmp_path, include_positive=False)
    result = evaluate_broker_paper_readiness(
        {"evidence_root": roots["evidence_root"], "research_export_root": roots["research_export_root"]},
        _policy(),
    )
    assert result["status"] == "BROKER_PAPER_NOT_READY"


def test_no_negative_path_not_ready(tmp_path):
    roots = _build_chain(tmp_path, include_negative=False)
    result = evaluate_broker_paper_readiness(
        {"evidence_root": roots["evidence_root"], "research_export_root": roots["research_export_root"]},
        _policy(),
    )
    assert result["status"] == "BROKER_PAPER_NOT_READY"


def test_live_flag_not_ready(tmp_path):
    roots = _build_chain(tmp_path, live_flag=True)
    result = evaluate_broker_paper_readiness(
        {"evidence_root": roots["evidence_root"], "research_export_root": roots["research_export_root"]},
        _policy(),
    )
    assert result["status"] == "BROKER_PAPER_NOT_READY"


def test_complete_chain_ready(tmp_path):
    roots = _build_chain(tmp_path)
    result = evaluate_broker_paper_readiness(
        {"evidence_root": roots["evidence_root"], "research_export_root": roots["research_export_root"]},
        _policy(),
    )
    assert result["status"] == "BROKER_PAPER_READY"


def test_same_inputs_same_output(tmp_path):
    roots = _build_chain(tmp_path)
    evidence_roots = {"evidence_root": roots["evidence_root"], "research_export_root": roots["research_export_root"]}
    r1 = evaluate_broker_paper_readiness(evidence_roots, _policy())
    r2 = evaluate_broker_paper_readiness(evidence_roots, _policy())
    assert r1["status"] == r2["status"]
    assert r1["checks"] == r2["checks"]
    assert r1["summary"] == r2["summary"]
