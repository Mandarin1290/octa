from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training
from octa_training.core.pipeline import PipelineResult


def _write_dummy_parquet(path: Path) -> None:
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2020-01-01", periods=10, freq="D", tz="UTC"),
            "open": [1.0] * 10,
            "high": [1.1] * 10,
            "low": [0.9] * 10,
            "close": [1.0] * 10,
            "volume": [1000.0] * 10,
        }
    )
    df.to_parquet(path, index=False)


def _write_cfg(path: Path, *, proof_mode: bool) -> None:
    path.write_text(
        (
            f"proof_mode: {'true' if proof_mode else 'false'}\n"
            "paths:\n"
            "  raw_dir: /home/n-b/Octa/raw\n"
            "  pkl_dir: /tmp/octa_test_pkl\n"
            "  logs_dir: /tmp/octa_test_logs\n"
            "  state_dir: /tmp/octa_test_state\n"
            "  reports_dir: /tmp/octa_test_reports\n"
            "cascade_timeframes: ['1D']\n"
        ),
        encoding="utf-8",
    )


def test_run_cascade_training_enables_proof_mode_fast_path(monkeypatch, tmp_path: Path) -> None:
    parquet_path = tmp_path / "AAA_1D.parquet"
    cfg_path = tmp_path / "proof.yaml"
    _write_dummy_parquet(parquet_path)
    _write_cfg(cfg_path, proof_mode=True)
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "octa_ops.autopilot.cascade_train._walkforward_eligibility",
        lambda **_kwargs: {"eligible": True, "reason": None},
    )

    def fake_train_evaluate_package(**kwargs):
        captured.update({"fast": kwargs.get("fast"), "robustness_profile": kwargs.get("robustness_profile")})
        return PipelineResult(
            symbol="AAA",
            run_id="r1",
            passed=True,
            metrics=SimpleNamespace(model_dump=lambda: {"n_trades": 25, "sharpe": 1.0, "max_drawdown": 0.02, "cagr": 0.1}),
            gate_result=SimpleNamespace(model_dump=lambda: {"passed": True, "details": {}}),
            pack_result={"model_artifacts": ["/tmp/fake.pkl"], "features_used": [], "altdata_sources_used": [], "altdata_enabled": False},
        )

    monkeypatch.setattr("octa_ops.autopilot.cascade_train.train_evaluate_adaptive", fake_train_evaluate_package)
    decisions, _ = run_cascade_training(
        run_id="r1",
        config_path=str(cfg_path),
        symbol="AAA",
        asset_class="equities",
        parquet_paths={"1D": str(parquet_path)},
        cascade=CascadePolicy(order=["1D"]),
        safe_mode=False,
        reports_dir=str(tmp_path / "reports"),
    )
    assert decisions[0].status == "PASS"
    assert captured["fast"] is True
    assert captured["robustness_profile"] == "risk_overlay"


def test_run_cascade_training_keeps_default_full_path_without_proof_mode(monkeypatch, tmp_path: Path) -> None:
    parquet_path = tmp_path / "AAA_1D.parquet"
    cfg_path = tmp_path / "standard.yaml"
    _write_dummy_parquet(parquet_path)
    _write_cfg(cfg_path, proof_mode=False)
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "octa_ops.autopilot.cascade_train._walkforward_eligibility",
        lambda **_kwargs: {"eligible": True, "reason": None},
    )

    def fake_train_evaluate_package(**kwargs):
        captured.update({"fast": kwargs.get("fast"), "robustness_profile": kwargs.get("robustness_profile")})
        return PipelineResult(
            symbol="AAA",
            run_id="r1",
            passed=True,
            metrics=SimpleNamespace(model_dump=lambda: {"n_trades": 25, "sharpe": 1.0, "max_drawdown": 0.02, "cagr": 0.1}),
            gate_result=SimpleNamespace(model_dump=lambda: {"passed": True, "details": {}}),
            pack_result={"model_artifacts": ["/tmp/fake.pkl"], "features_used": [], "altdata_sources_used": [], "altdata_enabled": False},
        )

    monkeypatch.setattr("octa_ops.autopilot.cascade_train.train_evaluate_adaptive", fake_train_evaluate_package)
    decisions, _ = run_cascade_training(
        run_id="r1",
        config_path=str(cfg_path),
        symbol="AAA",
        asset_class="equities",
        parquet_paths={"1D": str(parquet_path)},
        cascade=CascadePolicy(order=["1D"]),
        safe_mode=False,
        reports_dir=str(tmp_path / "reports"),
    )
    assert decisions[0].status == "PASS"
    assert captured["fast"] is False
    assert captured["robustness_profile"] == "full"
