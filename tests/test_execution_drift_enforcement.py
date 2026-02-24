from __future__ import annotations

import json
from pathlib import Path

import pytest

from octa.execution.runner import ExecutionConfig, run_execution


def _write_stage(tf: str, status: str, metrics: dict | None) -> dict:
    return {"timeframe": tf, "status": status, "metrics_summary": metrics}


def _make_minimal_selection(base: Path) -> None:
    run = base / "run_drift"
    (run / "preflight").mkdir(parents=True, exist_ok=True)
    (run / "preflight" / "summary.json").write_text("{}", encoding="utf-8")
    (run / "results").mkdir(parents=True, exist_ok=True)
    (run / "results" / "AAA.json").write_text(
        json.dumps(
            {
                "symbol": "AAA",
                "asset_class": "equities",
                "stages": [
                    _write_stage("1D", "PASS", {"n_trades": 1}),
                    _write_stage("1H", "PASS", {"n_trades": 1}),
                    _write_stage("30M", "PASS", {"n_trades": 1}),
                ],
            }
        ),
        encoding="utf-8",
    )


def test_drift_breach_blocks_paper(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
    (drift_dir / "global_1D_default.json").write_text(
        json.dumps({"disabled": True, "reason": "drift_breach", "streak": 3}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 120000.0, "currency": "USD"},
    )

    out_dir = tmp_path / "execution_run"
    with pytest.raises(RuntimeError, match="DRIFT_BREACH_BLOCK"):
        run_execution(
            ExecutionConfig(
                mode="paper",
                evidence_dir=out_dir,
                state_dir=tmp_path / "state",
                drift_registry_dir=drift_dir,
                base_evidence_dir=base,
                max_symbols=1,
                loop=False,
            )
        )

    incident = json.loads((out_dir / "drift_breach_block.json").read_text(encoding="utf-8"))
    assert incident["reason"] == "DRIFT_BREACH_BLOCK"
    assert len(incident["breaches"]) == 1


def test_drift_breach_warns_shadow_and_continues(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
    (drift_dir / "global_1D_default.json").write_text(
        json.dumps({"disabled": True, "reason": "drift_breach", "streak": 3}),
        encoding="utf-8",
    )

    out_dir = tmp_path / "execution_run"
    summary = run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=out_dir,
            state_dir=tmp_path / "state",
            drift_registry_dir=drift_dir,
            base_evidence_dir=base,
            max_symbols=1,
            loop=False,
        )
    )
    assert summary["mode"] == "dry-run"
    assert (out_dir / "drift_breach_block.json").exists()


def test_non_disabled_drift_state_does_not_block_paper(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
    (drift_dir / "global_1D_default.json").write_text(
        json.dumps({"disabled": False, "reason": "ok", "streak": 0}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 150000.0, "currency": "USD"},
    )

    out_dir = tmp_path / "execution_run"
    run_execution(
        ExecutionConfig(
            mode="paper",
            evidence_dir=out_dir,
            state_dir=tmp_path / "state",
            drift_registry_dir=drift_dir,
            base_evidence_dir=base,
            max_symbols=1,
            loop=False,
        )
    )
    assert not (out_dir / "drift_breach_block.json").exists()
