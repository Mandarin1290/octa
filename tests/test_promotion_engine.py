from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from octa.core.data.recycling.common import sha256_file
from octa.core.pipeline.promotion_runner import run_promotion
from octa.core.promotion.promotion_engine import evaluate_promotion
from octa.core.promotion.promotion_policy import PromotionPolicy


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")


def _make_shadow_evidence(tmp_path: Path, *, metrics_override: dict | None = None) -> Path:
    evidence_dir = tmp_path / "shadow_evidence"
    evidence_dir.mkdir()

    trades = pd.DataFrame(
        [
            {
                "trade_id": 1,
                "direction": "long",
                "entry_time": "2026-02-02T00:00:00+00:00",
                "exit_time": "2026-02-03T00:00:00+00:00",
                "entry_price": 100.0,
                "exit_price": 103.0,
                "quantity": 10.0,
                "entry_fee": 1.0,
                "exit_fee": 1.0,
                "gross_pnl": 30.0,
                "net_pnl": 28.0,
            }
        ]
    ).set_index("trade_id")
    equity = pd.DataFrame(
        {
            "cash": [100000.0, 99999.0, 100028.0],
            "equity": [100000.0, 100010.0, 100028.0],
            "position": [0, 1, 0],
            "quantity": [0.0, 10.0, 0.0],
            "drawdown": [0.0, 0.0, 0.0],
            "kill_switch": [False, False, False],
        },
        index=pd.to_datetime(
            ["2026-02-01T00:00:00Z", "2026-02-02T00:00:00Z", "2026-02-03T00:00:00Z"],
            utc=True,
        ),
    )
    trades.to_parquet(evidence_dir / "trades.parquet")
    equity.to_parquet(evidence_dir / "equity_curve.parquet")

    metrics = {
        "total_return": 0.02,
        "sharpe": 1.1,
        "max_drawdown": -0.03,
        "win_rate": 0.6,
        "profit_factor": 1.5,
        "n_trades": 20,
        "kill_switch_triggered": False,
    }
    if metrics_override:
        metrics.update(metrics_override)
    _write_json(evidence_dir / "metrics.json", metrics)
    _write_json(evidence_dir / "shadow_config.json", {"capital": 100000.0, "position_size": 0.5})
    _write_json(evidence_dir / "validation_report.json", {"status": "ok"})
    (evidence_dir / "sample_trades.txt").write_text("sample\n", encoding="utf-8")
    (evidence_dir / "sample_equity_head.txt").write_text("sample\n", encoding="utf-8")

    manifest = {
        "run_id": "shadow_fixture",
        "research_validation": {"status": "ok"},
        "shadow_validation": {"status": "ok"},
        "hashes": {},
    }
    for name in (
        "trades.parquet",
        "equity_curve.parquet",
        "metrics.json",
        "shadow_config.json",
        "sample_trades.txt",
        "sample_equity_head.txt",
    ):
        manifest["hashes"][name] = sha256_file(evidence_dir / name)
    _write_json(evidence_dir / "run_manifest.json", manifest)
    return evidence_dir


def _policy() -> PromotionPolicy:
    return PromotionPolicy(
        min_trades=10,
        min_win_rate=0.5,
        min_profit_factor=1.2,
        max_drawdown=0.10,
        min_total_return=0.01,
        require_hash_integrity=True,
        require_validation_ok=True,
    )


def test_good_shadow_metrics_are_eligible(tmp_path: Path) -> None:
    evidence_dir = _make_shadow_evidence(tmp_path)
    decision = evaluate_promotion(evidence_dir, _policy())
    assert decision["status"] == "PROMOTE_ELIGIBLE"
    assert all(item["status"] == "pass" for item in decision["checks"])


def test_bad_drawdown_is_blocked(tmp_path: Path) -> None:
    evidence_dir = _make_shadow_evidence(tmp_path, metrics_override={"max_drawdown": -0.25})
    decision = evaluate_promotion(evidence_dir, _policy())
    assert decision["status"] == "PROMOTE_BLOCKED"
    assert any(item["name"] == "max_drawdown" and item["status"] == "fail" for item in decision["checks"])


def test_missing_file_is_blocked(tmp_path: Path) -> None:
    evidence_dir = _make_shadow_evidence(tmp_path)
    (evidence_dir / "metrics.json").unlink()
    decision = evaluate_promotion(evidence_dir, _policy())
    assert decision["status"] == "PROMOTE_BLOCKED"
    assert decision["summary"]["blocked_by_validation"] is True


def test_manipulated_hash_is_blocked(tmp_path: Path) -> None:
    evidence_dir = _make_shadow_evidence(tmp_path)
    (evidence_dir / "sample_trades.txt").write_text("tampered\n", encoding="utf-8")
    decision = evaluate_promotion(evidence_dir, _policy())
    assert decision["status"] == "PROMOTE_BLOCKED"
    assert decision["summary"]["blocked_by_validation"] is True


def test_deterministic_inputs_produce_same_output(tmp_path: Path) -> None:
    evidence_dir = _make_shadow_evidence(tmp_path)
    first = evaluate_promotion(evidence_dir, _policy())
    second = evaluate_promotion(evidence_dir, _policy())
    assert first == second


def test_runner_writes_reports(tmp_path: Path) -> None:
    evidence_dir = _make_shadow_evidence(tmp_path)
    result = run_promotion(
        shadow_evidence_dir=evidence_dir,
        policy=_policy(),
        evidence_root=tmp_path / "promotion_evidence",
        run_id="promotion_run_fixture",
    )
    assert result["status"] == "PROMOTE_ELIGIBLE"
    assert (tmp_path / "promotion_evidence" / "promotion_run_fixture" / "decision_report.json").exists()
