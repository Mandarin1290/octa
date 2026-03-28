"""Tests for the NAV high-water mark drawdown circuit breaker (Module 3)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from octa.execution.runner import (
    ExecutionConfig,
    _load_nav_hwm,
    _save_nav_hwm,
    run_execution,
)


# ---------------------------------------------------------------------------
# Unit tests: HWM helpers
# ---------------------------------------------------------------------------


def test_load_nav_hwm_returns_zero_when_file_missing(tmp_path: Path) -> None:
    assert _load_nav_hwm(tmp_path) == 0.0


def test_load_nav_hwm_returns_zero_when_file_corrupt(tmp_path: Path) -> None:
    (tmp_path / "nav_hwm.json").write_text("not json", encoding="utf-8")
    assert _load_nav_hwm(tmp_path) == 0.0


def test_save_and_reload_nav_hwm(tmp_path: Path) -> None:
    _save_nav_hwm(tmp_path, 123456.78)
    assert abs(_load_nav_hwm(tmp_path) - 123456.78) < 1e-6


def test_save_nav_hwm_creates_state_dir(tmp_path: Path) -> None:
    state_dir = tmp_path / "nested" / "state"
    _save_nav_hwm(state_dir, 50000.0)
    assert state_dir.exists()
    assert abs(_load_nav_hwm(state_dir) - 50000.0) < 1e-6


# ---------------------------------------------------------------------------
# Helpers shared by integration tests
# ---------------------------------------------------------------------------


def _write_stage(tf: str, status: str, metrics: dict | None) -> dict:
    return {"timeframe": tf, "status": status, "metrics_summary": metrics}


def _make_minimal_selection(base: Path) -> None:
    run = base / "run_dd"
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


# ---------------------------------------------------------------------------
# Integration tests: circuit breaker behaviour
# ---------------------------------------------------------------------------


def test_no_drawdown_on_first_run_no_hwm_file(tmp_path: Path, monkeypatch) -> None:
    """First run with no HWM file: drawdown = 0, execution completes normally."""
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 100000.0, "currency": "USD", "positions": []},
    )

    state_dir = tmp_path / "state"
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
    run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=tmp_path / "out",
            state_dir=state_dir,
            drift_registry_dir=drift_dir,
            base_evidence_dir=base,
            max_symbols=1,
            loop=False,
            enable_carry=False,
        )
    )
    # HWM should have been written
    assert abs(_load_nav_hwm(state_dir) - 100000.0) < 1e-6
    # No circuit-breaker evidence file
    assert not (tmp_path / "out" / "drawdown_circuit_breaker.json").exists()


def test_hwm_updated_when_nav_exceeds_previous_hwm(tmp_path: Path, monkeypatch) -> None:
    """HWM is updated upward when current NAV is higher."""
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    state_dir = tmp_path / "state"
    _save_nav_hwm(state_dir, 80000.0)  # old HWM below current NAV

    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 100000.0, "currency": "USD", "positions": []},
    )

    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
    run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=tmp_path / "out",
            state_dir=state_dir,
            drift_registry_dir=drift_dir,
            base_evidence_dir=base,
            max_symbols=1,
            loop=False,
            enable_carry=False,
        )
    )
    assert abs(_load_nav_hwm(state_dir) - 100000.0) < 1e-6


def test_shadow_continues_and_emits_warning_on_drawdown_breach(tmp_path: Path, monkeypatch) -> None:
    """Shadow/dry-run: drawdown > 15 % → warning emitted, execution continues (no raise)."""
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    state_dir = tmp_path / "state"
    # Persist a HWM that puts current NAV ~20 % below peak
    _save_nav_hwm(state_dir, 125000.0)

    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 100000.0, "currency": "USD", "positions": []},
    )

    out_dir = tmp_path / "out"
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
    # Should not raise
    run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=out_dir,
            state_dir=state_dir,
            drift_registry_dir=drift_dir,
            base_evidence_dir=base,
            max_symbols=1,
            loop=False,
            enable_carry=False,
        )
    )
    evidence = json.loads((out_dir / "drawdown_circuit_breaker.json").read_text(encoding="utf-8"))
    assert evidence["reason"] == "DRAWDOWN_LIMIT_BREACH"
    assert evidence["portfolio_drawdown"] > 0.15
    assert evidence["hwm_nav"] == pytest.approx(125000.0)
    assert evidence["nav"] == pytest.approx(100000.0)
    # HWM must NOT be lowered
    assert abs(_load_nav_hwm(state_dir) - 125000.0) < 1e-6


def test_paper_raises_on_drawdown_breach(tmp_path: Path, monkeypatch) -> None:
    """Paper mode: drawdown > 15 % → RuntimeError('DRAWDOWN_LIMIT_BREACH')."""
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    state_dir = tmp_path / "state"
    _save_nav_hwm(state_dir, 125000.0)  # current NAV will be 100 000 → 20 % drawdown

    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 100000.0, "currency": "USD", "positions": []},
    )

    out_dir = tmp_path / "out"
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(RuntimeError, match="DRAWDOWN_LIMIT_BREACH"):
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
    evidence = json.loads((out_dir / "drawdown_circuit_breaker.json").read_text(encoding="utf-8"))
    assert evidence["reason"] == "DRAWDOWN_LIMIT_BREACH"
    assert evidence["mode"] == "paper"


def test_drawdown_below_limit_is_not_triggered(tmp_path: Path, monkeypatch) -> None:
    """Drawdown clearly below limit (14 %) does not trigger the circuit breaker."""
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    state_dir = tmp_path / "state"
    # 14 % drawdown: hwm=100000, nav=86000 → well below the 15 % limit
    _save_nav_hwm(state_dir, 100000.0)

    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 86000.0, "currency": "USD", "positions": []},
    )

    out_dir = tmp_path / "out"
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
    # Should not raise
    run_execution(
        ExecutionConfig(
            mode="dry-run",
            evidence_dir=out_dir,
            state_dir=state_dir,
            drift_registry_dir=drift_dir,
            base_evidence_dir=base,
            max_symbols=1,
            loop=False,
            enable_carry=False,
        )
    )
    assert not (out_dir / "drawdown_circuit_breaker.json").exists()
