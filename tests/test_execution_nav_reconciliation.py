from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from octa.execution.runner import ExecutionConfig, run_execution


def _write_stage(tf: str, status: str, metrics: dict | None) -> dict:
    return {"timeframe": tf, "status": status, "metrics_summary": metrics}


def _make_minimal_selection(base: Path) -> None:
    run = base / "run_nav"
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


def test_paper_uses_broker_nav_and_writes_snapshot(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 250000.0, "currency": "USD", "positions": []},
    )

    out_dir = tmp_path / "execution_run"
    state_dir = tmp_path / "state"
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
    run_execution(
        ExecutionConfig(
            mode="paper",
            evidence_dir=out_dir,
            state_dir=state_dir,
            drift_registry_dir=drift_dir,
            base_evidence_dir=base,
            max_symbols=1,
            loop=False,
            enable_carry=False,
        )
    )

    snap = json.loads((state_dir / "nav_snapshot.json").read_text(encoding="utf-8"))
    assert snap["mode"] == "paper"
    assert snap["source"] == "broker"
    assert snap["nav"] == 250000.0
    payload_wo_hash = dict(snap)
    snap_hash = str(payload_wo_hash.pop("hash"))
    expected_hash = hashlib.sha256(
        json.dumps(payload_wo_hash, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    ).hexdigest()
    assert snap_hash == expected_hash
    assert not (out_dir / "nav_reconcile_failed.json").exists()


def test_paper_blocks_when_broker_nav_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"positions": [], "buying_power": 0.0},
    )

    out_dir = tmp_path / "execution_run"
    state_dir = tmp_path / "state"
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(RuntimeError, match="NAV_RECONCILE_FAILED"):
        run_execution(
            ExecutionConfig(
                mode="paper",
                evidence_dir=out_dir,
                state_dir=state_dir,
                drift_registry_dir=drift_dir,
                base_evidence_dir=base,
                max_symbols=1,
                loop=False,
                enable_carry=False,
            )
        )

    incident = json.loads((out_dir / "nav_reconcile_failed.json").read_text(encoding="utf-8"))
    assert incident["reason"] == "NAV_RECONCILE_FAILED"


def test_shadow_fallback_when_broker_nav_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"positions": [], "buying_power": 0.0},
    )

    out_dir = tmp_path / "execution_run"
    state_dir = tmp_path / "state"
    run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=out_dir,
            state_dir=state_dir,
            base_evidence_dir=base,
            max_symbols=1,
            loop=False,
            enable_carry=False,
        )
    )

    snap = json.loads((state_dir / "nav_snapshot.json").read_text(encoding="utf-8"))
    assert snap["mode"] == "shadow"
    assert snap["source"] == "fallback"
    assert snap["nav"] == 100000.0
