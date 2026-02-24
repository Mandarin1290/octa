from __future__ import annotations

import json
from pathlib import Path

from octa.core.portfolio.preflight import PreflightResult
from octa.execution.runner import ExecutionConfig, run_execution


def _write_stage(tf: str, status: str, metrics: dict | None) -> dict:
    return {"timeframe": tf, "status": status, "metrics_summary": metrics}


def _make_minimal_selection(base: Path) -> None:
    run = base / "run_x"
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


def _run_with_preflight_reason(tmp_path: Path, monkeypatch, reason: str) -> Path:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    def _blocked_preflight(*args, **kwargs):
        return PreflightResult(
            ok=False,
            blocked_symbols=["AAA"],
            checks={"gate": reason},
            reason=reason,
        )

    monkeypatch.setattr("octa.execution.runner.run_preflight", _blocked_preflight)
    out_dir = tmp_path / "execution_run"
    summary = run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=out_dir,
            base_evidence_dir=base,
            loop=False,
            max_symbols=1,
            asset_class="equities",
            enable_carry=False,
        )
    )
    assert summary["portfolio_preflight_ok"] is False
    return out_dir


def test_correlation_exceeded_blocks_order_and_writes_incident(tmp_path: Path, monkeypatch) -> None:
    out_dir = _run_with_preflight_reason(tmp_path, monkeypatch, "CORRELATION_EXCEEDED")
    ml_orders = json.loads((out_dir / "ml_orders.json").read_text(encoding="utf-8"))
    assert ml_orders == []
    incident = json.loads((out_dir / "preflight_block.json").read_text(encoding="utf-8"))
    assert incident["reason"] == "CORRELATION_EXCEEDED"


def test_tail_risk_exceeded_blocks_order_and_writes_incident(tmp_path: Path, monkeypatch) -> None:
    out_dir = _run_with_preflight_reason(tmp_path, monkeypatch, "TAIL_RISK_EXCEEDED")
    ml_orders = json.loads((out_dir / "ml_orders.json").read_text(encoding="utf-8"))
    assert ml_orders == []
    incident = json.loads((out_dir / "preflight_block.json").read_text(encoding="utf-8"))
    assert incident["reason"] == "TAIL_RISK_EXCEEDED"
