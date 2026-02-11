from __future__ import annotations

import json
from types import SimpleNamespace
from pathlib import Path

from octa.core.cascade.policies import DEFAULT_TIMEFRAMES
from octa.support.ops.run_full_cascade_training_from_parquets import (
    RunSettings,
    _normalize_decisions,
    run_full_cascade,
)


def _mandatory_checks(passed: bool = True) -> dict:
    return {
        "monte_carlo": {"passed": passed},
        "walk_forward": {"passed": passed},
        "regime_stability": {"passed": passed},
        "cost_stress": {"passed": passed},
        "liquidity": {"passed": passed},
    }


def _write_preflight_fixture(preflight_dir: Path) -> None:
    preflight_dir.mkdir(parents=True, exist_ok=True)
    (preflight_dir / "trainable_symbols.txt").write_text("AAA\nBBB\n", encoding="utf-8")
    inventory_lines = [
        {
            "symbol": "AAA",
            "tfs": {tf: [str(preflight_dir / f"AAA_{tf}.parquet")] for tf in DEFAULT_TIMEFRAMES},
        },
        {
            "symbol": "BBB",
            "tfs": {tf: [str(preflight_dir / f"BBB_{tf}.parquet")] for tf in DEFAULT_TIMEFRAMES},
        },
    ]
    (preflight_dir / "inventory.jsonl").write_text(
        "\n".join(json.dumps(l) for l in inventory_lines) + "\n", encoding="utf-8"
    )


def test_run_full_cascade_batches_and_resume(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)

    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=0,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    for tf in DEFAULT_TIMEFRAMES:
        (tmp_path / f"{tf}.pkl").write_text("ok", encoding="utf-8")

    def stub_train_fn(**kwargs):
        decisions = [
            SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES
        ]
        metrics_by_tf = {
            tf: {
                "metrics": {"n_trades": 1, "sharpe": 1.0, "max_drawdown": 0.03, "cagr": 0.05},
                "model_artifacts": [str(tmp_path / f"{tf}.pkl")],
                **_mandatory_checks(True),
            }
            for tf in DEFAULT_TIMEFRAMES
        }
        return decisions, metrics_by_tf

    summary = run_full_cascade(settings, train_fn=stub_train_fn)
    assert summary["total_trainable"] == 2
    assert summary["passed"] == 2

    manifest_path = evidence_dir / "manifest.jsonl"
    lines = manifest_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2

    # Resume should not add new entries
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=0,
        resume=True,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )
    run_full_cascade(settings, train_fn=stub_train_fn)
    lines2 = manifest_path.read_text(encoding="utf-8").splitlines()
    assert len(lines2) == 2


def test_cascade_order_is_strict() -> None:
    assert list(DEFAULT_TIMEFRAMES) == ["1D", "1H", "30M", "5M", "1M"]


def test_gate_failed_preserves_reason_with_metrics() -> None:
    decisions = [
        SimpleNamespace(timeframe="1D", status="FAIL", reason="gate_failed"),
    ]
    metrics_by_tf = {
        "1D": {
            "metrics": {"n_trades": 10, "sharpe": 1.1},
            "model_artifacts": ["x"],
            "gate": {"passed": False, "failed_checks": ["pf"], "thresholds": {"pf": 1.2}},
        },
    }
    stages, ok, top_reason, top_detail = _normalize_decisions(decisions, metrics_by_tf)
    assert ok is False
    assert stages[0]["reason"] == "gate_failed"
    assert top_reason == "gate_failed_1D"
    assert top_detail
    failed_checks = top_detail["gate_summary"]["failed_checks"]
    assert failed_checks and failed_checks[0]["metric"] == "pf"
    assert failed_checks[0]["threshold"] == 1.2


def test_pass_requires_artifacts() -> None:
    decisions = [SimpleNamespace(timeframe="1D", status="PASS", reason=None)]
    metrics_by_tf = {"1D": {"metrics": {"n_trades": 10, "sharpe": 1.0}, "model_artifacts": []}}
    stages, ok, top_reason, _ = _normalize_decisions(decisions, metrics_by_tf)
    assert ok is False
    assert stages[0]["reason"] == "missing_model_artifacts"
    assert top_reason == "missing_model_artifacts_1D"


def test_summary_written_on_exception(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=1,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    def boom_train_fn(**kwargs):
        raise RuntimeError("boom")

    run_full_cascade(settings, train_fn=boom_train_fn)
    assert (evidence_dir / "summary.json").exists()
    assert (evidence_dir / "hashes.sha256").exists()


def test_symbols_override_records_missing(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=0,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
        symbols_override=["AAA", "ZZZ"],
    )

    for tf in DEFAULT_TIMEFRAMES:
        (tmp_path / f"{tf}.pkl").write_text("ok", encoding="utf-8")

    def stub_train_fn(**kwargs):
        decisions = [
            SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES
        ]
        metrics_by_tf = {
            tf: {
                "metrics": {"n_trades": 1, "sharpe": 1.0, "max_drawdown": 0.03, "cagr": 0.05},
                "model_artifacts": [str(tmp_path / f"{tf}.pkl")],
                **_mandatory_checks(True),
            }
            for tf in DEFAULT_TIMEFRAMES
        }
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    missing = json.loads((evidence_dir / "results" / "ZZZ.json").read_text(encoding="utf-8"))
    assert missing["status"] == "FAIL"
    assert missing["reason"] == "symbol_not_trainable_or_missing"


def test_gate_failed_logs_failed_checks(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=1,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    def stub_train_fn(**kwargs):
        decisions = [SimpleNamespace(timeframe="1D", status="FAIL", reason="gate_failed")]
        metrics_by_tf = {
            "1D": {
                "metrics": {"n_trades": 10, "sharpe": 0.1},
                "model_artifacts": ["x"],
                "gate": {"passed": False, "failed_checks": ["pf", "sharpe"], "thresholds": {"pf": 1.2}},
            }
        }
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    failed_checks = result["stages"][0]["gate_summary"]["failed_checks"]
    assert failed_checks
    assert {fc["metric"] for fc in failed_checks} == {"pf", "sharpe"}
    log_text = (evidence_dir / "logs" / "runner.log").read_text(encoding="utf-8")
    assert "failed_checks_count=2" in log_text
    assert "thresholds_summary" in log_text


def test_train_error_writes_error_artifact(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=1,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    def boom_train_fn(**kwargs):
        raise RuntimeError("boom")

    run_full_cascade(settings, train_fn=boom_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    stage = result["stages"][0]
    assert stage["reason"] == "train_error"
    assert stage["error_type"] == "RuntimeError"
    exception_ref = stage["exception_ref"]
    assert (evidence_dir / exception_ref["exception_json"]).exists()
    assert (evidence_dir / exception_ref["traceback_txt"]).exists()
    detail_ref = result["detail"]["exception_ref"]
    assert detail_ref == exception_ref

    payload = json.loads((evidence_dir / exception_ref["exception_json"]).read_text(encoding="utf-8"))
    assert payload["exception_type"] == "RuntimeError"
    assert payload["symbol"] == "AAA"
    assert payload["timeframe"] == "1D"


def test_gate_failed_unexplained_fails_closed(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=1,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    def stub_train_fn(**kwargs):
        decisions = [SimpleNamespace(timeframe="1D", status="FAIL", reason="gate_failed")]
        metrics_by_tf = {
            "1D": {
                "metrics": {"n_trades": 10, "sharpe": 0.1},
                "model_artifacts": ["x"],
                "gate": {"passed": False},
            }
        }
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    stage = result["stages"][0]
    assert stage["reason"] == "gate_failed_unexplained"
    assert stage["gate_summary"]["unexplained"] is True
    assert stage["gate_unexplained_ref"]
    assert (evidence_dir / stage["gate_unexplained_ref"]).exists()


def test_monte_carlo_mandatory_missing_fails_stage(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=1,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    artifact = tmp_path / "ok.pkl"
    artifact.write_text("ok", encoding="utf-8")

    def stub_train_fn(**kwargs):
        decisions = [SimpleNamespace(timeframe="1D", status="PASS", reason=None)]
        metrics_by_tf = {"1D": {"metrics": {"n_trades": 10, "sharpe": 1.0}, "model_artifacts": [str(artifact)]}}
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert result["status"] == "FAIL"
    assert result["stages"][0]["reason"] == "monte_carlo_missing"


def test_walkforward_mandatory_missing_fails_stage(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=1,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    artifact = tmp_path / "ok.pkl"
    artifact.write_text("ok", encoding="utf-8")

    def stub_train_fn(**kwargs):
        decisions = [SimpleNamespace(timeframe="1D", status="PASS", reason=None)]
        checks = _mandatory_checks(True)
        checks.pop("walk_forward")
        metrics_by_tf = {"1D": {"metrics": {"n_trades": 10, "sharpe": 1.0}, "model_artifacts": [str(artifact)], **checks}}
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert result["status"] == "FAIL"
    assert result["stages"][0]["reason"] == "walkforward_missing"


def test_train_error_from_decision_writes_exception_artifacts(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=1,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    def stub_train_fn(**kwargs):
        decisions = [SimpleNamespace(timeframe="1D", status="FAIL", reason="train_error", details={"error": "inner boom"})]
        metrics_by_tf = {"1D": {"metrics": {"n_trades": 10}, "model_artifacts": ["x"]}}
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    stage = result["stages"][0]
    assert stage["reason"] == "train_error"
    exception_ref = stage["exception_ref"]
    assert (evidence_dir / exception_ref["exception_json"]).exists()
    assert (evidence_dir / exception_ref["traceback_txt"]).exists()
    assert result["detail"]["exception_ref"] == exception_ref


def test_paper_ready_requires_1d_and_1h_with_mc_pass(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=1,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    for tf in DEFAULT_TIMEFRAMES:
        (tmp_path / f"{tf}.pkl").write_text("ok", encoding="utf-8")

    def stub_train_fn(**kwargs):
        decisions = [
            SimpleNamespace(timeframe="1D", status="PASS", reason=None),
            SimpleNamespace(timeframe="1H", status="FAIL", reason="gate_failed"),
            SimpleNamespace(timeframe="30M", status="SKIP", reason="cascade_previous_not_pass"),
            SimpleNamespace(timeframe="5M", status="SKIP", reason="cascade_previous_not_pass"),
            SimpleNamespace(timeframe="1M", status="SKIP", reason="cascade_previous_not_pass"),
        ]
        metrics_by_tf = {
            "1D": {"metrics": {"n_trades": 100, "sharpe": 1.0}, "model_artifacts": [str(tmp_path / "1D.pkl")], **_mandatory_checks(True)},
            "1H": {"metrics": {"n_trades": 100, "sharpe": 0.1}, "model_artifacts": [str(tmp_path / "1H.pkl")], **_mandatory_checks(False)},
        }
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert result["paper_ready"] is False


def test_paper_ready_true_when_1d_1h_and_mc_pass(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=1,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    for tf in DEFAULT_TIMEFRAMES:
        (tmp_path / f"{tf}.pkl").write_text("ok", encoding="utf-8")

    def stub_train_fn(**kwargs):
        decisions = [SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES]
        metrics_by_tf = {
            tf: {"metrics": {"n_trades": 120, "sharpe": 1.0, "max_drawdown": 0.03, "cagr": 0.05}, "model_artifacts": [str(tmp_path / f"{tf}.pkl")], **_mandatory_checks(True)}
            for tf in DEFAULT_TIMEFRAMES
        }
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert result["paper_ready"] is True


def test_cross_tf_inconsistent_fails_symbol(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=1,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    for tf in DEFAULT_TIMEFRAMES:
        (tmp_path / f"{tf}.pkl").write_text("ok", encoding="utf-8")

    def stub_train_fn(**kwargs):
        decisions = [SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES]
        metrics_by_tf = {
            "1D": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.12, "max_drawdown": 0.05}, "model_artifacts": [str(tmp_path / "1D.pkl")], **_mandatory_checks(True)},
            "1H": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": -0.08, "max_drawdown": 0.04}, "model_artifacts": [str(tmp_path / "1H.pkl")], **_mandatory_checks(True)},
            "30M": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.01, "max_drawdown": 0.03}, "model_artifacts": [str(tmp_path / "30M.pkl")], **_mandatory_checks(True)},
            "5M": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.01, "max_drawdown": 0.03}, "model_artifacts": [str(tmp_path / "5M.pkl")], **_mandatory_checks(True)},
            "1M": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.01, "max_drawdown": 0.02}, "model_artifacts": [str(tmp_path / "1M.pkl")], **_mandatory_checks(True)},
        }
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert result["status"] == "FAIL"
    assert result["reason"] == "cross_tf_inconsistent"
    assert result["cross_tf_meta"]["passed"] is False


def test_cross_tf_aligned_passes(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=1,
        max_symbols=1,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    for tf in DEFAULT_TIMEFRAMES:
        (tmp_path / f"{tf}.pkl").write_text("ok", encoding="utf-8")

    def stub_train_fn(**kwargs):
        decisions = [SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES]
        metrics_by_tf = {
            "1D": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.12, "max_drawdown": 0.05}, "model_artifacts": [str(tmp_path / "1D.pkl")], **_mandatory_checks(True)},
            "1H": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.08, "max_drawdown": 0.04}, "model_artifacts": [str(tmp_path / "1H.pkl")], **_mandatory_checks(True)},
            "30M": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.01, "max_drawdown": 0.03}, "model_artifacts": [str(tmp_path / "30M.pkl")], **_mandatory_checks(True)},
            "5M": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.01, "max_drawdown": 0.03}, "model_artifacts": [str(tmp_path / "5M.pkl")], **_mandatory_checks(True)},
            "1M": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.01, "max_drawdown": 0.02}, "model_artifacts": [str(tmp_path / "1M.pkl")], **_mandatory_checks(True)},
        }
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert result["status"] == "PASS"
    assert result["cross_tf_meta"]["passed"] is True
