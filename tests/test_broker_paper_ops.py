import json
import hashlib
from pathlib import Path

import pandas as pd

from octa.core.data.recycling.common import sha256_file
from octa.core.operations.broker_paper_ops_engine import execute_broker_paper_ops
from octa.core.operations.broker_paper_ops_planner import plan_broker_paper_runs
from octa.core.operations.broker_paper_ops_policy import BrokerPaperOpsPolicy
from octa.core.pipeline.broker_paper_ops_runner import run_broker_paper_ops


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2, allow_nan=True), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _manifest_for(directory: Path, file_names: list[str]) -> dict:
    return {name: sha256_file(directory / name) for name in file_names}


def _build_ops_fixture(base: Path, *, broker_mode: str = "PAPER") -> dict[str, Path]:
    evidence_root = base / "evidence"
    export_root = base / "research_exports"
    export_dir = export_root / "research_bridge_20260320T222511Z"
    export_dir.mkdir(parents=True, exist_ok=True)
    index = pd.date_range("2026-02-01", periods=8, freq="D", tz="UTC")
    signals = pd.DataFrame(
        {
            "signal": [0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0],
            "signal_strength": [0.0, 1.0, 1.0, 0.0, 0.0, 0.8, 0.0, 0.0],
        },
        index=index,
    )
    returns = pd.DataFrame({"returns": [0.0, 0.01, 0.012, 0.004, 0.009, 0.012, 0.005, 0.002]}, index=index)
    signals.to_parquet(export_dir / "signals.parquet")
    returns.to_parquet(export_dir / "returns.parquet")
    _write_json(export_dir / "metadata.json", {"strategy_name": "x", "timeframe": "1D", "params": {}, "source": "unit"})
    files = {
        "metadata.json": {
            "path": str((export_dir / "metadata.json").resolve()),
            "sha256": sha256_file(export_dir / "metadata.json"),
        },
        "returns.parquet": {
            "path": str((export_dir / "returns.parquet").resolve()),
            "sha256": sha256_file(export_dir / "returns.parquet"),
        },
        "signals.parquet": {
            "path": str((export_dir / "signals.parquet").resolve()),
            "sha256": sha256_file(export_dir / "signals.parquet"),
        },
    }
    bundle_payload = json.dumps(
        {
            "files": {
                "metadata.json": files["metadata.json"]["sha256"],
                "returns.parquet": files["returns.parquet"]["sha256"],
                "signals.parquet": files["signals.parquet"]["sha256"],
            },
            "run_id": export_dir.name,
            "source_env_prefix": "/tmp/test-research-env",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    _write_json(
        export_dir / "export_manifest.json",
        {
            "bundle_sha256": hashlib.sha256(bundle_payload.encode("utf-8")).hexdigest(),
            "files": files,
            "source_env": {"prefix": "/tmp/test-research-env"},
        },
    )

    shadow = evidence_root / "shadow_run_20260321T073916Z"
    _write_text(shadow / "shadow_config.json", "{}")
    _write_text(shadow / "trades.parquet", "shadow-trades")
    _write_text(shadow / "equity_curve.parquet", "shadow-equity")
    _write_json(shadow / "metrics.json", {"n_trades": 2})
    _write_text(shadow / "sample_trades.txt", "sample")
    _write_text(shadow / "sample_equity_head.txt", "sample")
    _write_json(
        shadow / "run_manifest.json",
        {
            "hashes": _manifest_for(shadow, ["shadow_config.json", "trades.parquet", "equity_curve.parquet", "metrics.json", "sample_trades.txt", "sample_equity_head.txt"]),
            "research_export_path": str(export_dir.resolve()),
        },
    )

    promotion = evidence_root / "promotion_run_20260321T073916Z"
    _write_json(promotion / "decision_report.json", {"status": "PROMOTE_ELIGIBLE", "summary": {}, "input_evidence_dir": str(shadow.resolve())})
    _write_json(promotion / "applied_policy.json", {"x": 1})
    _write_text(promotion / "promotion_summary.txt", "ok\n")
    _write_json(promotion / "evidence_manifest.json", {"hashes": _manifest_for(promotion, ["decision_report.json", "applied_policy.json", "promotion_summary.txt"])})

    paper_gate = evidence_root / "paper_gate_20260321T073916Z"
    _write_json(paper_gate / "paper_gate_report.json", {"gate_result": {"status": "PAPER_ELIGIBLE"}, "promotion_evidence_dir": str(promotion.resolve())})
    _write_json(paper_gate / "applied_paper_policy.json", {"require_promotion_status": "PROMOTE_ELIGIBLE"})
    _write_text(paper_gate / "paper_gate_summary.txt", "ok\n")
    _write_json(paper_gate / "evidence_manifest.json", {"hashes": _manifest_for(paper_gate, ["paper_gate_report.json", "applied_paper_policy.json", "paper_gate_summary.txt"])})

    paper_session = evidence_root / "paper_session_20260321T073916Z"
    references = {
        "paper_gate_evidence_dir": str(paper_gate.resolve()),
        "promotion_evidence_dir": str(promotion.resolve()),
        "shadow_evidence_dir": str(shadow.resolve()),
        "research_export_path": str(export_dir.resolve()),
    }
    _write_json(
        paper_session / "paper_session_report.json",
        {
            "blocked_reason": None,
            "references": references,
            "session_policy": {"require_gate_status": "PAPER_ELIGIBLE"},
            "session_summary": {"status": "PAPER_SESSION_COMPLETED", "symbols": ["TEST"]},
            "validation_result": {"status": "ok", "n_trades": 2, "kill_switch_triggered": False},
        },
    )
    _write_json(
        paper_session / "session_manifest.json",
        {
            "blocked_reason": None,
            "metrics": {"final_equity": 101000.0, "kill_switch_triggered": False, "max_drawdown": 0.01, "n_trades": 2},
            "references": references,
            "session_summary": {"status": "PAPER_SESSION_COMPLETED", "symbols": ["TEST"]},
            "validation_result": {"status": "ok"},
        },
    )
    _write_json(paper_session / "session_policy.json", {"mode": "PAPER"})
    _write_text(paper_session / "trades.parquet", "trades")
    _write_text(paper_session / "equity_curve.parquet", "equity")
    _write_json(paper_session / "session_metrics.json", {"final_equity": 101000.0, "kill_switch_triggered": False, "max_drawdown": 0.01, "n_trades": 2})
    _write_text(paper_session / "sample_trades.txt", "sample")
    _write_text(paper_session / "sample_equity_head.txt", "sample")
    _write_json(
        paper_session / "evidence_manifest.json",
        {"hashes": _manifest_for(paper_session, ["paper_session_report.json", "session_manifest.json", "session_policy.json", "trades.parquet", "equity_curve.parquet", "session_metrics.json", "sample_trades.txt", "sample_equity_head.txt"])},
    )

    broker = evidence_root / "broker_paper_20260321T080502Z"
    policy = {
        "allow_short": False,
        "forbid_live_mode": True,
        "kill_switch_drawdown": 0.2,
        "max_allowed_drawdown": 0.01,
        "max_open_positions": 1,
        "max_session_age_hours": 1000.0,
        "paper_capital": 100000.0,
        "paper_fee": 0.0005,
        "paper_slippage": 0.0002,
        "require_broker_mode": broker_mode,
        "require_hash_integrity": True,
        "require_kill_switch_not_triggered": True,
        "require_min_completed_sessions": 1,
        "require_min_profit_factor": 0.0,
        "require_min_total_trades": 2,
        "require_min_win_rate": 0.0,
        "require_paper_gate_status": "PAPER_ELIGIBLE",
    }
    _write_json(
        broker / "broker_paper_report.json",
        {
            "blocked_reason": None,
            "references": {
                "paper_gate_evidence_dir": str(paper_gate.resolve()),
                "paper_session_evidence_dir": str(paper_session.resolve()),
                "promotion_evidence_dir": str(promotion.resolve()),
                "shadow_evidence_dir": str(shadow.resolve()),
                "research_export_path": str(export_dir.resolve()),
            },
            "policy": policy,
            "gate_result": {"status": "BROKER_PAPER_ELIGIBLE", "checks": []},
            "validation_result": {"status": "ok", "n_orders": 5, "n_fills": 5, "kill_switch_triggered": False},
            "session_summary": {"status": "BROKER_PAPER_SESSION_COMPLETED", "symbols": ["TEST"]},
        },
    )
    _write_json(broker / "applied_broker_paper_policy.json", policy)
    _write_text(broker / "orders.parquet", "orders")
    _write_text(broker / "fills.parquet", "fills")
    _write_text(broker / "positions.parquet", "positions")
    _write_text(broker / "equity_curve.parquet", "equity")
    _write_json(
        broker / "metrics.json",
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
    _write_text(broker / "sample_orders.txt", "sample")
    _write_text(broker / "sample_fills.txt", "sample")
    _write_text(broker / "sample_equity_head.txt", "sample")
    _write_json(
        broker / "evidence_manifest.json",
        {
            "hashes": _manifest_for(
                broker,
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

    readiness = evidence_root / "broker_paper_readiness_20260321T090626Z"
    inventory = {
        "roots": {"evidence_root": str(evidence_root.resolve()), "research_export_root": str(export_root.resolve())},
        "chains": [
            {
                "broker_paper_evidence_dir": str(broker.resolve()),
                "status": "BROKER_PAPER_SESSION_COMPLETED",
                "chain_complete": True,
                "references": {
                    "paper_gate_evidence_dir": str(paper_gate.resolve()),
                    "paper_session_evidence_dir": str(paper_session.resolve()),
                    "promotion_evidence_dir": str(promotion.resolve()),
                    "shadow_evidence_dir": str(shadow.resolve()),
                    "research_export_path": str(export_dir.resolve()),
                },
            }
        ],
        "summary": {"n_broker_paper_runs": 1, "n_complete_chains": 1, "n_incomplete_chains": 0},
    }
    _write_json(readiness / "readiness_inventory.json", inventory)
    _write_json(
        readiness / "readiness_governance_report.json",
        {"status": "ok", "checks": [], "summary": {"negative_path_proof": True, "positive_path_proof": True, "paper_only_enforced": True, "no_live_flags": True, "all_chain_complete": True, "kill_switch_path_tested": True}},
    )
    _write_json(
        readiness / "readiness_metrics_report.json",
        {"status": "ok", "summary": {"completed_broker_paper_sessions": 1, "blocked_broker_paper_runs": 0, "max_observed_drawdown": 0.005, "risks": ["Non-finite metric detected"]}},
    )
    _write_json(
        readiness / "readiness_report.json",
        {"status": "BROKER_PAPER_NOT_READY", "checks": [], "summary": {"policy": {"x": 1}}},
    )
    _write_json(readiness / "applied_readiness_policy.json", {"x": 1})
    _write_json(
        readiness / "evidence_manifest.json",
        {"hashes": _manifest_for(readiness, ["readiness_inventory.json", "readiness_governance_report.json", "readiness_metrics_report.json", "readiness_report.json", "applied_readiness_policy.json"])},
    )
    return {"evidence_root": evidence_root, "readiness": readiness}


def _policy() -> BrokerPaperOpsPolicy:
    return BrokerPaperOpsPolicy(
        require_readiness_status="BROKER_PAPER_READY",
        allow_runs_when_not_ready=True,
        max_runs_per_batch=1,
        max_session_duration_minutes=60,
        min_cooldown_seconds_between_runs=0,
        paper_only=True,
        forbid_live_mode=True,
        stop_on_first_failure=True,
        max_consecutive_failures=1,
        require_evidence_integrity=True,
    )


def test_missing_readiness_evidence_blocked(tmp_path):
    result = run_broker_paper_ops(
        readiness_evidence_dir=tmp_path / "missing",
        policy=_policy(),
        evidence_root=tmp_path / "evidence",
        run_id="broker_paper_ops_blocked",
    )
    assert result["status"] == "OPS_BLOCKED"


def test_valid_plan_ready(tmp_path):
    fx = _build_ops_fixture(tmp_path)
    plan = plan_broker_paper_runs(fx["readiness"], _policy())
    assert plan["status"] == "OPS_PLAN_READY"
    assert len(plan["planned_runs"]) == 1


def test_stop_on_first_failure(tmp_path):
    fx = _build_ops_fixture(tmp_path, broker_mode="LIVE")
    plan = plan_broker_paper_runs(fx["readiness"], _policy())
    execution = execute_broker_paper_ops(
        {**plan, "policy": _policy().to_dict()},
        evidence_root=fx["evidence_root"],
        batch_run_id="ops_batch_fail",
    )
    assert execution["batch_status"] == "OPS_ABORTED"
    assert execution["runs"][0]["status"] == "BROKER_PAPER_SESSION_ABORTED"


def test_deterministic_inputs_same_batch_result(tmp_path):
    fx = _build_ops_fixture(tmp_path)
    plan = plan_broker_paper_runs(fx["readiness"], _policy())
    e1 = execute_broker_paper_ops({**plan, "policy": _policy().to_dict()}, evidence_root=fx["evidence_root"], batch_run_id="ops_batch_a")
    e2 = execute_broker_paper_ops({**plan, "policy": _policy().to_dict()}, evidence_root=fx["evidence_root"], batch_run_id="ops_batch_b")
    assert e1["batch_status"] == e2["batch_status"]
    assert e1["summary"]["aggregated_metrics"] == e2["summary"]["aggregated_metrics"]


def test_non_finite_metrics_flagged(tmp_path):
    fx = _build_ops_fixture(tmp_path)
    result = run_broker_paper_ops(
        readiness_evidence_dir=fx["readiness"],
        policy=_policy(),
        evidence_root=fx["evidence_root"],
        run_id="broker_paper_ops_nonfinite",
    )
    report = json.loads((Path(result["evidence_dir"]) / "aggregated_metrics.json").read_text())
    assert report["non_finite_metric_flags"]


def test_no_live_mode_allowed(tmp_path):
    fx = _build_ops_fixture(tmp_path, broker_mode="LIVE")
    result = run_broker_paper_ops(
        readiness_evidence_dir=fx["readiness"],
        policy=_policy(),
        evidence_root=fx["evidence_root"],
        run_id="broker_paper_ops_live_block",
    )
    assert result["status"] == "OPS_ABORTED"
