"""Tests for daily NAV loss limit and consecutive loss streak (Module 3)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
_YESTERDAY = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

from octa.execution.runner import (
    ExecutionConfig,
    _load_loss_streak,
    _load_nav_day_open,
    _save_loss_streak,
    _save_nav_day_open,
    run_execution,
)


# ---------------------------------------------------------------------------
# Unit tests: day-open NAV helpers
# ---------------------------------------------------------------------------


def test_load_nav_day_open_missing_file(tmp_path: Path) -> None:
    nav, date = _load_nav_day_open(tmp_path)
    assert nav == 0.0
    assert date == ""


def test_load_nav_day_open_corrupt_file(tmp_path: Path) -> None:
    (tmp_path / "nav_day_open.json").write_text("garbage", encoding="utf-8")
    nav, date = _load_nav_day_open(tmp_path)
    assert nav == 0.0
    assert date == ""


def test_save_and_reload_nav_day_open(tmp_path: Path) -> None:
    _save_nav_day_open(tmp_path, 99500.0, _TODAY)
    nav, date = _load_nav_day_open(tmp_path)
    assert abs(nav - 99500.0) < 1e-6
    assert date == _TODAY


# ---------------------------------------------------------------------------
# Unit tests: loss streak helpers
# ---------------------------------------------------------------------------


def test_load_loss_streak_missing(tmp_path: Path) -> None:
    assert _load_loss_streak(tmp_path) == 0


def test_save_and_reload_loss_streak(tmp_path: Path) -> None:
    _save_loss_streak(tmp_path, 3)
    assert _load_loss_streak(tmp_path) == 3


def test_save_loss_streak_zero(tmp_path: Path) -> None:
    _save_loss_streak(tmp_path, 5)
    _save_loss_streak(tmp_path, 0)
    assert _load_loss_streak(tmp_path) == 0


# ---------------------------------------------------------------------------
# Helpers for integration tests
# ---------------------------------------------------------------------------


def _write_stage(tf: str, status: str, metrics: dict | None) -> dict:
    return {"timeframe": tf, "status": status, "metrics_summary": metrics}


def _make_minimal_selection(base: Path) -> None:
    run = base / "run_dloss"
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
                ],
            }
        ),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Integration tests: kill switch fires on daily loss
# ---------------------------------------------------------------------------


def test_kill_switch_fires_when_daily_loss_exceeds_5pct(tmp_path: Path, monkeypatch) -> None:
    """Daily loss > 5 % triggers kill switch on first cycle."""
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    state_dir = tmp_path / "state"
    # Persist a day-open NAV well above current broker NAV → 10 % daily loss
    _save_nav_day_open(state_dir, 100000.0, _TODAY)

    monkeypatch.setattr(
        "octa.execution.runner._today_str_for_test",
        _TODAY,
        raising=False,
    )
    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 90000.0, "currency": "USD", "positions": []},
    )

    out_dir = tmp_path / "out"
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
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
    # Kill switch must have triggered — evidence file written
    ks_files = list(out_dir.glob("kill_switch_triggered_cycle_*.json"))
    assert ks_files, "Expected kill switch evidence file"
    ks_data = json.loads(ks_files[0].read_text(encoding="utf-8"))
    assert ks_data["reason"] == "DAILY_LOSS"
    assert ks_data["daily_loss_pct"] == pytest.approx(10.0, abs=0.01)


def test_no_kill_switch_on_first_run_no_day_open(tmp_path: Path, monkeypatch) -> None:
    """First run of the day (no day_open file): daily_loss = 0, no kill switch."""
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 100000.0, "currency": "USD", "positions": []},
    )

    state_dir = tmp_path / "state"
    out_dir = tmp_path / "out"
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
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
    ks_files = list(out_dir.glob("kill_switch_triggered_cycle_*.json"))
    assert not ks_files, "Kill switch must not fire on first run with no prior day_open"
    # Day open was persisted
    nav, date = _load_nav_day_open(state_dir)
    assert nav == pytest.approx(100000.0)


def test_day_open_resets_when_date_changes(tmp_path: Path, monkeypatch) -> None:
    """If persisted day_open is from a prior date, it is overwritten with today's NAV."""
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    state_dir = tmp_path / "state"
    # Persist yesterday's NAV (lower value) — should be ignored / reset today
    _save_nav_day_open(state_dir, 50000.0, _YESTERDAY)

    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 100000.0, "currency": "USD", "positions": []},
    )

    out_dir = tmp_path / "out"
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
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
    # Kill switch must NOT fire — the old day_open (50k) should not be used as today's open
    ks_files = list(out_dir.glob("kill_switch_triggered_cycle_*.json"))
    assert not ks_files


# ---------------------------------------------------------------------------
# Integration tests: loss streak tracking
# ---------------------------------------------------------------------------


def test_loss_streak_increments_when_nav_below_open(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    state_dir = tmp_path / "state"
    # Seed streak at 2
    _save_loss_streak(state_dir, 2)
    # Set day-open above current NAV (2 % loss, below kill threshold)
    _save_nav_day_open(state_dir, 100000.0, _TODAY)

    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 98000.0, "currency": "USD", "positions": []},
    )

    out_dir = tmp_path / "out"
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
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
    assert _load_loss_streak(state_dir) == 3
    assert (out_dir / "loss_streak.json").exists()


def test_loss_streak_resets_when_nav_at_or_above_open(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("OCTA_TELEGRAM_ENABLED", "false")
    base = tmp_path / "evidence_base"
    _make_minimal_selection(base)

    state_dir = tmp_path / "state"
    _save_loss_streak(state_dir, 4)
    _save_nav_day_open(state_dir, 100000.0, _TODAY)

    monkeypatch.setattr(
        "octa.execution.broker_router.BrokerRouter.account_snapshot",
        lambda self: {"net_liquidation": 100000.0, "currency": "USD", "positions": []},
    )

    out_dir = tmp_path / "out"
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
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
    assert _load_loss_streak(state_dir) == 0
    assert not (out_dir / "loss_streak.json").exists()
