from __future__ import annotations

import hashlib
import json
import pickle
import signal
import time
from types import SimpleNamespace
from pathlib import Path

from octa.core.cascade.policies import DEFAULT_TIMEFRAMES
from octa.support.ops.run_full_cascade_training_from_parquets import (
    RunSettings,
    _normalize_asset_class_filter,
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


def _write_tradeable_artifact_bundle(
    artifact_dir: Path,
    *,
    symbol: str,
    timeframe: str,
    artifact_kind: str = "tradeable",
    asset_class: str = "equities",
) -> list[str]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    pkl_path = artifact_dir / f"{symbol}_{timeframe}.pkl"
    meta_path = artifact_dir / f"{symbol}_{timeframe}.meta.json"
    sha_path = artifact_dir / f"{symbol}_{timeframe}.sha256"
    payload = {
        "artifact_kind": artifact_kind,
        "schema_version": 1,
        "asset": {"symbol": symbol, "asset_class": asset_class},
        "timeframe": timeframe,
    }
    blob = pickle.dumps(payload)
    pkl_path.write_bytes(blob)
    sha_path.write_text(hashlib.sha256(blob).hexdigest(), encoding="utf-8")
    meta_path.write_text(
        json.dumps(
            {
                "symbol": symbol,
                "asset_class": asset_class,
                "run_id": "test-run",
                "created_at": "2026-01-01T00:00:00Z",
                "artifact_kind": artifact_kind,
                "schema_version": 1,
                "metrics": {"n_trades": 1},
                "gate": {"passed": artifact_kind == "tradeable"},
                "feature_count": 1,
                "horizons": [timeframe],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return [str(pkl_path), str(meta_path), str(sha_path)]


def _write_preflight_fixture(preflight_dir: Path) -> None:
    preflight_dir.mkdir(parents=True, exist_ok=True)
    (preflight_dir / "trainable_symbols.txt").write_text("AAA\nBBB\n", encoding="utf-8")
    inventory_lines = [
        {
            "symbol": "AAA",
            "asset_class": "equities",
            "tfs": {tf: [str(preflight_dir / f"AAA_{tf}.parquet")] for tf in DEFAULT_TIMEFRAMES},
        },
        {
            "symbol": "BBB",
            "asset_class": "futures",
            "tfs": {tf: [str(preflight_dir / f"BBB_{tf}.parquet")] for tf in DEFAULT_TIMEFRAMES},
        },
    ]
    (preflight_dir / "inventory.jsonl").write_text(
        "\n".join(json.dumps(l) for l in inventory_lines) + "\n", encoding="utf-8"
    )


def spawned_success_train_fn(**kwargs):
    artifact_dir = Path(str(kwargs["model_root"]))
    decisions = [SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES]
    metrics_by_tf = {
        tf: {
            "metrics": {"n_trades": 1, "sharpe": 1.0, "max_drawdown": 0.03, "cagr": 0.05},
            "model_artifacts": _write_tradeable_artifact_bundle(
                artifact_dir,
                symbol=kwargs["symbol"],
                timeframe=tf,
                asset_class=str(kwargs.get("asset_class") or "equities"),
            ),
            **_mandatory_checks(True),
        }
        for tf in DEFAULT_TIMEFRAMES
    }
    return decisions, metrics_by_tf


def spawned_hanging_train_fn(**_kwargs):
    time.sleep(30.0)
    raise AssertionError("unreachable")


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

    def stub_train_fn(**kwargs):
        artifact_dir = tmp_path / kwargs["symbol"]
        decisions = [
            SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES
        ]
        metrics_by_tf = {
            tf: {
                "metrics": {"n_trades": 1, "sharpe": 1.0, "max_drawdown": 0.03, "cagr": 0.05},
                "model_artifacts": _write_tradeable_artifact_bundle(
                    artifact_dir,
                    symbol=kwargs["symbol"],
                    timeframe=tf,
                    asset_class=str(kwargs.get("asset_class") or "equities"),
                ),
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
    stages, ok, top_reason, top_detail = _normalize_decisions(decisions, metrics_by_tf, expected_symbol="AAA")
    assert ok is False
    assert stages[0]["status"] == "GATE_FAIL"
    assert stages[0]["reason"] == "gate_failed"
    assert top_reason == "gate_failed_1D"
    assert top_detail
    failed_checks = top_detail["gate_summary"]["failed_checks"]
    assert failed_checks and failed_checks[0]["metric"] == "pf"
    assert failed_checks[0]["threshold"] == 1.2


def test_pass_requires_artifacts() -> None:
    decisions = [SimpleNamespace(timeframe="1D", status="PASS", reason=None)]
    metrics_by_tf = {"1D": {"metrics": {"n_trades": 10, "sharpe": 1.0}, "model_artifacts": []}}
    stages, ok, top_reason, _ = _normalize_decisions(decisions, metrics_by_tf, expected_symbol="AAA")
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


def test_successful_run_writes_summary_json(tmp_path: Path) -> None:
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
        symbols_override=["AAA"],
    )

    def stub_train_fn(**kwargs):
        artifact_dir = tmp_path / kwargs["symbol"]
        decisions = [
            SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES
        ]
        metrics_by_tf = {
            tf: {
                "metrics": {"n_trades": 1, "sharpe": 1.0, "max_drawdown": 0.03, "cagr": 0.05},
                "model_artifacts": _write_tradeable_artifact_bundle(
                    artifact_dir,
                    symbol=kwargs["symbol"],
                    timeframe=tf,
                    asset_class=str(kwargs.get("asset_class") or "equities"),
                ),
                **_mandatory_checks(True),
            }
            for tf in DEFAULT_TIMEFRAMES
        }
        return decisions, metrics_by_tf

    summary = run_full_cascade(settings, train_fn=stub_train_fn)
    assert summary["final_verdict"] == "offline_training_ready"
    assert (evidence_dir / "summary.json").exists()


def test_sigterm_abort_writes_failure_result_and_summary(tmp_path: Path) -> None:
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
        symbols_override=["AAA"],
    )

    def aborting_train_fn(**_kwargs):
        import os

        os.kill(os.getpid(), signal.SIGTERM)
        raise AssertionError("unreachable")

    summary = run_full_cascade(settings, train_fn=aborting_train_fn)
    assert summary["final_verdict"] == "run_failed"
    assert summary["exit_code"] != 0
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert result["status"] == "FAIL"
    assert result["reason"] == "aborted_fail_closed"
    assert result["training_outcome"] == "failed"
    assert (evidence_dir / "summary.json").exists()


def test_spawned_symbol_success_returns_to_runner_and_writes_summary(tmp_path: Path) -> None:
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
        symbols_override=["AAA"],
        symbol_timeout_sec=10,
    )

    summary = run_full_cascade(settings, train_fn=spawned_success_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert summary["final_verdict"] == "offline_training_ready"
    assert summary["exit_code"] == 0
    assert result["status"] == "PASS"
    assert (evidence_dir / "summary.json").exists()


def test_spawned_symbol_timeout_writes_failure_result_and_summary(tmp_path: Path) -> None:
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
        symbols_override=["AAA"],
        symbol_timeout_sec=1,
    )

    summary = run_full_cascade(settings, train_fn=spawned_hanging_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert summary["final_verdict"] == "completed_with_symbol_failures"
    assert summary["exit_code"] == 1
    assert summary["timed_out"] == 1
    assert result["status"] == "FAIL"
    assert result["reason"] == "symbol_training_timeout"
    assert result["training_outcome"] == "failed"
    assert not result["model_artifacts"] if "model_artifacts" in result else True
    assert (evidence_dir / "summary.json").exists()


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

    def stub_train_fn(**kwargs):
        artifact_dir = tmp_path / kwargs["symbol"]
        decisions = [
            SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES
        ]
        metrics_by_tf = {
            tf: {
                "metrics": {"n_trades": 1, "sharpe": 1.0, "max_drawdown": 0.03, "cagr": 0.05},
                "model_artifacts": _write_tradeable_artifact_bundle(
                    artifact_dir,
                    symbol=kwargs["symbol"],
                    timeframe=tf,
                    asset_class=str(kwargs.get("asset_class") or "equities"),
                ),
                **_mandatory_checks(True),
            }
            for tf in DEFAULT_TIMEFRAMES
        }
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    missing = json.loads((evidence_dir / "results" / "ZZZ.json").read_text(encoding="utf-8"))
    assert missing["status"] == "FAIL"
    assert missing["reason"] == "symbol_not_trainable_or_missing"

def test_asset_class_filtering_and_stage_metadata(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=10,
        max_symbols=0,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
        asset_classes=("equities",),
    )

    seen_asset_classes = []

    def stub_train_fn(**kwargs):
        seen_asset_classes.append(str(kwargs.get("asset_class")))
        artifact_dir = tmp_path / kwargs["symbol"]
        decisions = [SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES]
        metrics_by_tf = {
            tf: {
                "asset_class": str(kwargs.get("asset_class")),
                "metrics": {"n_trades": 1, "sharpe": 1.0, "max_drawdown": 0.03, "cagr": 0.05},
                "model_artifacts": _write_tradeable_artifact_bundle(
                    artifact_dir,
                    symbol=kwargs["symbol"],
                    timeframe=tf,
                    asset_class=str(kwargs.get("asset_class") or "equities"),
                ),
                **_mandatory_checks(True),
            }
            for tf in DEFAULT_TIMEFRAMES
        }
        return decisions, metrics_by_tf

    summary = run_full_cascade(settings, train_fn=stub_train_fn)
    assert summary["total_trainable"] == 1
    assert summary["passed"] == 1
    assert seen_asset_classes == ["equities"]

    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert result["asset_class"] == "equities"
    assert all(str(stage.get("asset_class")) == "equities" for stage in result["stages"])

    log_text = (evidence_dir / "logs" / "runner.log").read_text(encoding="utf-8")
    assert "[info] selected_asset_classes=['equities'] before_count=2 after_count=1" in log_text
    assert "[train] symbol=AAA asset_class=equities" in log_text


def test_asset_class_filter_normalizes_aliases() -> None:
    assert _normalize_asset_class_filter(["equity", "stocks", "equities"]) == ("equities",)
    assert _normalize_asset_class_filter(["future", "futures"]) == ("futures",)
    assert _normalize_asset_class_filter([]) is None


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
    assert stage["status"] == "TRAIN_ERROR"
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
    assert stage["status"] == "GATE_FAIL"
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

    def stub_train_fn(**kwargs):
        decisions = [SimpleNamespace(timeframe="1D", status="PASS", reason=None)]
        metrics_by_tf = {
            "1D": {
                "metrics": {"n_trades": 10, "sharpe": 1.0},
                "model_artifacts": _write_tradeable_artifact_bundle(
                    tmp_path / "artifacts",
                    symbol=kwargs["symbol"],
                    timeframe="1D",
                    asset_class=str(kwargs.get("asset_class") or "equities"),
                ),
            }
        }
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

    def stub_train_fn(**kwargs):
        decisions = [SimpleNamespace(timeframe="1D", status="PASS", reason=None)]
        checks = _mandatory_checks(True)
        checks.pop("walk_forward")
        metrics_by_tf = {
            "1D": {
                "metrics": {"n_trades": 10, "sharpe": 1.0},
                "model_artifacts": _write_tradeable_artifact_bundle(
                    tmp_path / "artifacts",
                    symbol=kwargs["symbol"],
                    timeframe="1D",
                    asset_class=str(kwargs.get("asset_class") or "equities"),
                ),
                **checks,
            }
        }
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
        decisions = [SimpleNamespace(timeframe="1D", status="TRAIN_ERROR", reason="train_error", details={"error": "inner boom"})]
        metrics_by_tf = {"1D": {"metrics": {"n_trades": 10}, "model_artifacts": ["x"]}}
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    stage = result["stages"][0]
    assert stage["status"] == "TRAIN_ERROR"
    assert stage["reason"] == "train_error"
    exception_ref = stage["exception_ref"]
    assert (evidence_dir / exception_ref["exception_json"]).exists()
    assert (evidence_dir / exception_ref["traceback_txt"]).exists()
    assert result["detail"]["exception_ref"] == exception_ref


def test_gate_fail_status_does_not_write_exception_artifact(tmp_path: Path) -> None:
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
        decisions = [SimpleNamespace(timeframe="1D", status="GATE_FAIL", reason="walkforward_failed", details={"error": "gate_fail"})]
        metrics_by_tf = {"1D": {"metrics": {"n_trades": 10, "sharpe": 0.1}, "model_artifacts": ["x"], "gate": {"passed": False, "reasons": ["walkforward_failed"]}}}
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    stage = result["stages"][0]
    assert stage["status"] == "GATE_FAIL"
    assert stage["reason"] == "walkforward_failed"
    assert "exception_ref" not in stage


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

    def stub_train_fn(**kwargs):
        artifact_dir = tmp_path / kwargs["symbol"]
        decisions = [
            SimpleNamespace(timeframe="1D", status="PASS", reason=None),
            SimpleNamespace(timeframe="1H", status="FAIL", reason="gate_failed"),
            SimpleNamespace(timeframe="30M", status="SKIP", reason="cascade_previous_not_pass"),
            SimpleNamespace(timeframe="5M", status="SKIP", reason="cascade_previous_not_pass"),
            SimpleNamespace(timeframe="1M", status="SKIP", reason="cascade_previous_not_pass"),
        ]
        metrics_by_tf = {
            "1D": {
                "metrics": {"n_trades": 100, "sharpe": 1.0},
                "model_artifacts": _write_tradeable_artifact_bundle(
                    artifact_dir,
                    symbol=kwargs["symbol"],
                    timeframe="1D",
                    asset_class=str(kwargs.get("asset_class") or "equities"),
                ),
                **_mandatory_checks(True),
            },
            "1H": {
                "metrics": {"n_trades": 100, "sharpe": 0.1},
                "model_artifacts": _write_tradeable_artifact_bundle(
                    artifact_dir,
                    symbol=kwargs["symbol"],
                    timeframe="1H",
                    asset_class=str(kwargs.get("asset_class") or "equities"),
                ),
                **_mandatory_checks(False),
            },
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

    def stub_train_fn(**kwargs):
        artifact_dir = tmp_path / kwargs["symbol"]
        decisions = [SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES]
        metrics_by_tf = {
            tf: {
                "metrics": {"n_trades": 120, "sharpe": 1.0, "max_drawdown": 0.03, "cagr": 0.05},
                "model_artifacts": _write_tradeable_artifact_bundle(
                    artifact_dir,
                    symbol=kwargs["symbol"],
                    timeframe=tf,
                    asset_class=str(kwargs.get("asset_class") or "equities"),
                ),
                **_mandatory_checks(True),
            }
            for tf in DEFAULT_TIMEFRAMES
        }
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert result["paper_ready"] is True


def test_foundation_validation_regime_blocks_paper_ready(tmp_path: Path, monkeypatch) -> None:
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
        config_path="configs/foundation_validation.yaml",
        skip_preflight=True,
    )

    def stub_train_fn(**kwargs):
        artifact_dir = tmp_path / kwargs["symbol"]
        decisions = [SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES]
        metrics_by_tf = {
            tf: {
                "metrics": {"n_trades": 10, "sharpe": 1.0, "max_drawdown": 0.03, "cagr": 0.05},
                "model_artifacts": _write_tradeable_artifact_bundle(
                    artifact_dir,
                    symbol=kwargs["symbol"],
                    timeframe=tf,
                    asset_class=str(kwargs.get("asset_class") or "equities"),
                ),
                **_mandatory_checks(True),
            }
            for tf in DEFAULT_TIMEFRAMES
        }
        return decisions, metrics_by_tf

    summary = run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert summary["training_regime"] == "foundation_validation"
    assert result["training_regime"] == "foundation_validation"
    assert result["paper_ready"] is False
    assert result["paper_block_reason"] == "paper_promotion_blocked_for_regime:foundation_validation"


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

    def stub_train_fn(**kwargs):
        artifact_dir = tmp_path / kwargs["symbol"]
        decisions = [SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES]
        metrics_by_tf = {
            "1D": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.12, "max_drawdown": 0.05}, "model_artifacts": _write_tradeable_artifact_bundle(artifact_dir, symbol=kwargs["symbol"], timeframe="1D", asset_class=str(kwargs.get("asset_class") or "equities")), **_mandatory_checks(True)},
            "1H": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": -0.08, "max_drawdown": 0.04}, "model_artifacts": _write_tradeable_artifact_bundle(artifact_dir, symbol=kwargs["symbol"], timeframe="1H", asset_class=str(kwargs.get("asset_class") or "equities")), **_mandatory_checks(True)},
            "30M": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.01, "max_drawdown": 0.03}, "model_artifacts": _write_tradeable_artifact_bundle(artifact_dir, symbol=kwargs["symbol"], timeframe="30M", asset_class=str(kwargs.get("asset_class") or "equities")), **_mandatory_checks(True)},
            "5M": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.01, "max_drawdown": 0.03}, "model_artifacts": _write_tradeable_artifact_bundle(artifact_dir, symbol=kwargs["symbol"], timeframe="5M", asset_class=str(kwargs.get("asset_class") or "equities")), **_mandatory_checks(True)},
            "1M": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.01, "max_drawdown": 0.02}, "model_artifacts": _write_tradeable_artifact_bundle(artifact_dir, symbol=kwargs["symbol"], timeframe="1M", asset_class=str(kwargs.get("asset_class") or "equities")), **_mandatory_checks(True)},
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

    def stub_train_fn(**kwargs):
        artifact_dir = tmp_path / kwargs["symbol"]
        decisions = [SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES]
        metrics_by_tf = {
            "1D": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.12, "max_drawdown": 0.05}, "model_artifacts": _write_tradeable_artifact_bundle(artifact_dir, symbol=kwargs["symbol"], timeframe="1D", asset_class=str(kwargs.get("asset_class") or "equities")), **_mandatory_checks(True)},
            "1H": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.08, "max_drawdown": 0.04}, "model_artifacts": _write_tradeable_artifact_bundle(artifact_dir, symbol=kwargs["symbol"], timeframe="1H", asset_class=str(kwargs.get("asset_class") or "equities")), **_mandatory_checks(True)},
            "30M": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.01, "max_drawdown": 0.03}, "model_artifacts": _write_tradeable_artifact_bundle(artifact_dir, symbol=kwargs["symbol"], timeframe="30M", asset_class=str(kwargs.get("asset_class") or "equities")), **_mandatory_checks(True)},
            "5M": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.01, "max_drawdown": 0.03}, "model_artifacts": _write_tradeable_artifact_bundle(artifact_dir, symbol=kwargs["symbol"], timeframe="5M", asset_class=str(kwargs.get("asset_class") or "equities")), **_mandatory_checks(True)},
            "1M": {"metrics": {"n_trades": 120, "sharpe": 1.0, "cagr": 0.01, "max_drawdown": 0.02}, "model_artifacts": _write_tradeable_artifact_bundle(artifact_dir, symbol=kwargs["symbol"], timeframe="1M", asset_class=str(kwargs.get("asset_class") or "equities")), **_mandatory_checks(True)},
        }
        return decisions, metrics_by_tf

    run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert result["status"] == "PASS"
    assert result["cross_tf_meta"]["passed"] is True


def test_debug_artifact_does_not_count_as_training_success(tmp_path: Path) -> None:
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
        decisions = [SimpleNamespace(timeframe="1D", status="PASS", reason=None)]
        metrics_by_tf = {
            "1D": {
                "metrics": {"n_trades": 10, "sharpe": 1.0},
                "model_artifacts": _write_tradeable_artifact_bundle(
                    tmp_path / "artifacts",
                    symbol=kwargs["symbol"],
                    timeframe="1D",
                    artifact_kind="debug",
                    asset_class=str(kwargs.get("asset_class") or "equities"),
                ),
                **_mandatory_checks(True),
            }
        }
        return decisions, metrics_by_tf

    summary = run_full_cascade(settings, train_fn=stub_train_fn)
    result = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    assert result["status"] == "FAIL"
    assert result["training_outcome"] == "trained_but_invalid"
    assert result["stages"][0]["artifact_validation"]["valid"] is False
    assert result["stages"][0]["reason"] == "artifact_kind_not_tradeable"
    assert summary["artifacts_invalid"] >= 1


def test_explicit_requested_symbols_fail_closed_when_none_valid(tmp_path: Path) -> None:
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
        symbols_override=[],
        symbols_requested_explicitly=True,
        symbols_file_path=str(tmp_path / "admitted_symbols.txt"),
    )

    def should_not_run(**kwargs):
        raise AssertionError("train_fn must not run when no requested symbols are valid")

    summary = run_full_cascade(settings, train_fn=should_not_run)
    report = json.loads((evidence_dir / "input_symbols_report.json").read_text(encoding="utf-8"))
    assert report["symbols_requested_explicitly"] is True
    assert summary["error"] == "no_valid_requested_symbols"
    assert summary["final_verdict"] == "blocked_fail_closed"
    assert summary["exit_code"] == 2


def test_symbols_override_report_tracks_invalid_duplicate_and_missing(tmp_path: Path) -> None:
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
        symbols_override=["AAA", "aaa", "BAD SYMBOL", "ZZZ"],
        symbols_requested_explicitly=True,
        symbols_file_path=str(tmp_path / "admitted_symbols.txt"),
    )

    def stub_train_fn(**kwargs):
        decisions = [SimpleNamespace(timeframe="1D", status="SKIP", reason="insufficient_history")]
        metrics_by_tf = {"1D": {"metrics": {"n_trades": 0}, "model_artifacts": []}}
        return decisions, metrics_by_tf

    summary = run_full_cascade(settings, train_fn=stub_train_fn)
    report = json.loads((evidence_dir / "input_symbols_report.json").read_text(encoding="utf-8"))
    invalid = json.loads((evidence_dir / "results" / "BAD SYMBOL.json").read_text(encoding="utf-8"))
    missing = json.loads((evidence_dir / "results" / "ZZZ.json").read_text(encoding="utf-8"))
    assert report["requested_count"] == 4
    assert report["accepted_count"] == 1
    assert report["duplicates_removed_count"] == 1
    assert report["invalid_format_count"] == 1
    assert report["missing_count"] == 1
    assert report["accepted_symbols"] == ["AAA"]
    assert invalid["training_outcome"] == "failed"
    assert missing["training_outcome"] == "skipped"
    assert summary["input_symbols_rejected"] == 2


def test_run_summary_matches_symbol_outcomes(tmp_path: Path) -> None:
    preflight_dir = tmp_path / "preflight"
    _write_preflight_fixture(preflight_dir)
    evidence_dir = tmp_path / "evidence"
    settings = RunSettings(
        root=tmp_path,
        preflight_out=preflight_dir,
        evidence_dir=evidence_dir,
        batch_size=10,
        max_symbols=0,
        resume=False,
        start_at=None,
        dry_run=False,
        config_path=None,
        skip_preflight=True,
    )

    def stub_train_fn(**kwargs):
        symbol = str(kwargs["symbol"])
        if symbol == "AAA":
            artifact_dir = tmp_path / "artifacts" / symbol
            return (
                [SimpleNamespace(timeframe=tf, status="PASS", reason=None) for tf in DEFAULT_TIMEFRAMES],
                {
                    tf: {
                        "metrics": {"n_trades": 10, "sharpe": 1.2, "cagr": 0.05, "max_drawdown": 0.03},
                        "model_artifacts": _write_tradeable_artifact_bundle(
                            artifact_dir,
                            symbol=symbol,
                            timeframe=tf,
                            asset_class=str(kwargs.get("asset_class") or "equities"),
                        ),
                        **_mandatory_checks(True),
                    }
                    for tf in DEFAULT_TIMEFRAMES
                },
            )
        return (
            [SimpleNamespace(timeframe="1D", status="TRAIN_ERROR", reason="train_error", details={"error": "boom"})],
            {"1D": {"metrics": {"n_trades": 0}, "model_artifacts": []}},
        )

    summary = run_full_cascade(settings, train_fn=stub_train_fn)
    aaa = json.loads((evidence_dir / "results" / "AAA.json").read_text(encoding="utf-8"))
    bbb = json.loads((evidence_dir / "results" / "BBB.json").read_text(encoding="utf-8"))
    assert aaa["training_outcome"] == "trained_successfully"
    assert bbb["training_outcome"] == "failed"
    assert summary["outcome_counts"]["trained_successfully"] == 1
    assert summary["outcome_counts"]["failed"] == 1
    assert summary["passed"] == 1
    assert summary["failed"] == 1
    assert summary["final_verdict"] == "completed_with_symbol_failures"
    assert summary["exit_code"] == 1


def test_artifact_asset_class_alias_stock_accepted_as_equities(tmp_path: Path) -> None:
    # Regression: infer_asset_class() writes "stock" into artifact meta, but
    # the universe inventory records "equities". Before the fix, this caused
    # artifact_asset_class_mismatch even when all performance gates passed.
    # After fix: ASSET_CLASS_ALIASES normalizes "stock" → "equities" before comparison.
    artifact_dir = tmp_path / "models"
    artifacts = _write_tradeable_artifact_bundle(
        artifact_dir, symbol="ASA", timeframe="1D", asset_class="stock"
    )
    decisions = [SimpleNamespace(timeframe="1D", status="PASS", reason=None)]
    metrics_by_tf = {
        "1D": {
            "metrics": {"n_trades": 10, "sharpe": 1.5, "max_drawdown": 0.03, "cagr": 0.10},
            "model_artifacts": artifacts,
            "asset_class": "equities",  # inventory value; artifact has "stock"
            **_mandatory_checks(True),
        }
    }
    stages, ok, top_reason, _ = _normalize_decisions(
        decisions, metrics_by_tf, expected_symbol="ASA", default_asset_class="equities",
        cascade_tfs=["1D"],
    )
    assert ok is True, f"Expected PASS but got top_reason={top_reason}, reason={stages[0].get('reason')}"
    assert stages[0]["status"] == "PASS"


def test_artifact_asset_class_genuine_mismatch_still_fails(tmp_path: Path) -> None:
    # A truly mismatched asset class (e.g. "futures" artifact in an equities slot) must still fail.
    artifact_dir = tmp_path / "models"
    artifacts = _write_tradeable_artifact_bundle(
        artifact_dir, symbol="ASA", timeframe="1D", asset_class="futures"
    )
    decisions = [SimpleNamespace(timeframe="1D", status="PASS", reason=None)]
    metrics_by_tf = {
        "1D": {
            "metrics": {"n_trades": 10, "sharpe": 1.5, "max_drawdown": 0.03, "cagr": 0.10},
            "model_artifacts": artifacts,
            "asset_class": "equities",
            **_mandatory_checks(True),
        }
    }
    stages, ok, top_reason, _ = _normalize_decisions(
        decisions, metrics_by_tf, expected_symbol="ASA", default_asset_class="equities",
        cascade_tfs=["1D"],
    )
    assert ok is False
    assert stages[0]["reason"] == "artifact_asset_class_mismatch"
