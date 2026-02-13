from __future__ import annotations

import json
from pathlib import Path

from octa.execution.runner import ExecutionConfig, run_execution


def _write_stage(tf: str, status: str, metrics: dict | None) -> dict:
    return {"timeframe": tf, "status": status, "metrics_summary": metrics}


def test_execution_e2e_dryrun_with_carry(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
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
                    _write_stage("5M", "SKIP", {"n_trades": 1}),
                    _write_stage("1M", "SKIP", {"n_trades": 1}),
                ],
            }
        ),
        encoding="utf-8",
    )

    carry_cfg = {
        "min_rate_diff": 0.005,
        "enter_threshold": 0.005,
        "exit_threshold": 0.002,
        "rebalance_interval_hours": 1,
        "min_hold_days": 0,
        "instruments": [
            {
                "instrument": "EURUSD",
                "base_ccy": "EUR",
                "quote_ccy": "USD",
                "asset_class": "fx_carry",
                "funding_cost": 0.001,
                "volatility": 0.1,
            }
        ],
    }
    carry_rates = {"rates": {"USD": 0.05, "EUR": 0.02}}
    carry_cfg_path = tmp_path / "carry_config.json"
    carry_rates_path = tmp_path / "carry_rates.json"
    carry_cfg_path.write_text(json.dumps(carry_cfg), encoding="utf-8")
    carry_rates_path.write_text(json.dumps(carry_rates), encoding="utf-8")

    out_dir = tmp_path / "execution_run"
    summary = run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=out_dir,
            base_evidence_dir=base,
            loop=True,
            cycle_seconds=0,
            max_cycles=2,
            max_symbols=5,
            asset_class="equities",
            enable_carry=True,
            carry_config_path=carry_cfg_path,
            carry_rates_path=carry_rates_path,
        )
    )
    assert summary["mode"] == "dry-run"
    assert (out_dir / "selection" / "eligible_symbols.json").exists()
    assert (out_dir / "cycle_001" / "carry_status.json").exists()
    assert (out_dir / "cycle_002" / "carry_status.json").exists()
    assert (out_dir / "notifications.jsonl").exists()
