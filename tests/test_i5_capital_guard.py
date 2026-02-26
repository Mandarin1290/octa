"""I5: Capital Guard — cross-run NAV persistence and discrepancy detection.

Tests that:
- CapitalState persists and loads correctly across runs.
- No discrepancy event is emitted when previous NAV is zero (initial state).
- No discrepancy event is emitted when discrepancy ≤ threshold.
- EVENT_GOVERNANCE_ENFORCED is emitted when discrepancy > threshold.
- Conservative NAV (max of broker and persisted) is used after the check.
- Capital state is saved at end of run with the updated NAV.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from octa.execution.capital_state import CapitalState, NAV_DISCREPANCY_THRESHOLD


# ── CapitalState unit tests ───────────────────────────────────────────────────

def test_load_or_init_returns_zero_when_no_file(tmp_path: Path) -> None:
    state = CapitalState.load_or_init(tmp_path)
    assert state.nav == 0.0
    assert state.source == "initial"
    assert state.timestamp_utc == ""


def test_save_and_reload(tmp_path: Path) -> None:
    original = CapitalState(nav=123456.78, timestamp_utc="2026-01-01T00:00:00Z", source="broker")
    original.save(tmp_path)
    loaded = CapitalState.load_or_init(tmp_path)
    assert loaded.nav == pytest.approx(123456.78)
    assert loaded.source == "broker"
    assert loaded.timestamp_utc == "2026-01-01T00:00:00Z"


def test_load_or_init_falls_back_on_corrupt_file(tmp_path: Path) -> None:
    (tmp_path / "capital_state.json").write_text("NOT JSON {{{", encoding="utf-8")
    state = CapitalState.load_or_init(tmp_path)
    assert state.nav == 0.0
    assert state.source == "initial"


def test_discrepancy_zero_when_no_previous_nav(tmp_path: Path) -> None:
    state = CapitalState(nav=0.0, timestamp_utc="", source="initial")
    assert state.discrepancy(100_000.0) == 0.0


def test_discrepancy_zero_when_broker_nav_zero() -> None:
    state = CapitalState(nav=100_000.0, timestamp_utc="2026-01-01T00:00:00Z", source="broker")
    assert state.discrepancy(0.0) == 0.0


def test_discrepancy_exact_calculation() -> None:
    state = CapitalState(nav=100_000.0, timestamp_utc="2026-01-01T00:00:00Z", source="broker")
    # 5% jump
    assert state.discrepancy(105_000.0) == pytest.approx(5_000 / 105_000)


def test_discrepancy_below_threshold() -> None:
    state = CapitalState(nav=100_000.0, timestamp_utc="2026-01-01T00:00:00Z", source="broker")
    assert state.discrepancy(100_500.0) < NAV_DISCREPANCY_THRESHOLD


def test_discrepancy_above_threshold() -> None:
    state = CapitalState(nav=100_000.0, timestamp_utc="2026-01-01T00:00:00Z", source="broker")
    assert state.discrepancy(115_000.0) > NAV_DISCREPANCY_THRESHOLD


def test_save_creates_parent_dirs(tmp_path: Path) -> None:
    nested = tmp_path / "a" / "b" / "c"
    CapitalState(nav=1.0, timestamp_utc="2026-01-01T00:00:00Z", source="broker").save(nested)
    assert (nested / "capital_state.json").exists()


def test_saved_json_is_valid(tmp_path: Path) -> None:
    CapitalState(nav=99.5, timestamp_utc="2026-01-01T00:00:00Z", source="fallback").save(tmp_path)
    data = json.loads((tmp_path / "capital_state.json").read_text())
    assert data["nav"] == pytest.approx(99.5)
    assert data["source"] == "fallback"


# ── runner integration tests ──────────────────────────────────────────────────

def _make_run_cfg(tmp_path: Path, mode: str = "shadow"):
    """Build an ExecutionConfig pointing at isolated tmp dirs."""
    from octa.execution.runner import ExecutionConfig
    evidence = tmp_path / "evidence"
    state_dir = tmp_path / "state"
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(parents=True, exist_ok=True)
    return ExecutionConfig(
        mode=mode,
        evidence_dir=evidence,
        base_evidence_dir=tmp_path / "base_evidence",
        state_dir=state_dir,
        drift_registry_dir=drift_dir,
    )


def _stub_broker(nav: float):
    broker = MagicMock()
    broker.account_snapshot.return_value = {"net_liquidation": nav, "currency": "USD"}
    broker.place_order.return_value = {"status": "ok"}
    return broker


def test_capital_state_saved_after_run(tmp_path: Path) -> None:
    """After a run, capital_state.json must exist in state_dir with updated NAV."""
    from octa.execution.runner import run_execution

    cfg = _make_run_cfg(tmp_path)
    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
    ):
        MockRouter.return_value = _stub_broker(nav=120_000.0)
        run_execution(cfg)

    state = CapitalState.load_or_init(cfg.state_dir)
    assert state.nav == pytest.approx(120_000.0)
    assert state.source == "broker"


def test_no_governance_event_on_initial_run(tmp_path: Path) -> None:
    """First run (no persisted state) → no NAV discrepancy event emitted."""
    from octa.execution.runner import run_execution
    from octa.core.governance.governance_audit import EVENT_GOVERNANCE_ENFORCED

    cfg = _make_run_cfg(tmp_path)
    emitted: list = []

    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
        patch("octa.execution.runner.GovernanceAudit") as MockAudit,
    ):
        mock_audit_instance = MagicMock()
        MockAudit.return_value = mock_audit_instance
        mock_audit_instance.emit.side_effect = lambda evt, payload: emitted.append((evt, payload))

        MockRouter.return_value = _stub_broker(nav=120_000.0)
        run_execution(cfg)

    enforced_events = [e for e, _ in emitted if e == EVENT_GOVERNANCE_ENFORCED]
    assert len(enforced_events) == 0, "No discrepancy event on first run (persisted nav=0)"


def test_governance_event_emitted_on_large_discrepancy(tmp_path: Path) -> None:
    """If persisted NAV differs from broker NAV by >1%, emit EVENT_GOVERNANCE_ENFORCED."""
    from octa.execution.runner import run_execution
    from octa.core.governance.governance_audit import EVENT_GOVERNANCE_ENFORCED

    cfg = _make_run_cfg(tmp_path)
    # Pre-seed capital state with a very different NAV
    CapitalState(nav=100_000.0, timestamp_utc="2026-01-01T00:00:00Z", source="broker").save(cfg.state_dir)

    emitted: list = []

    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
        patch("octa.execution.runner.GovernanceAudit") as MockAudit,
    ):
        mock_audit_instance = MagicMock()
        MockAudit.return_value = mock_audit_instance
        mock_audit_instance.emit.side_effect = lambda evt, payload: emitted.append((evt, payload))

        # Broker reports NAV 20% higher → discrepancy > 1%
        MockRouter.return_value = _stub_broker(nav=120_000.0)
        run_execution(cfg)

    enforced_events = [(e, p) for e, p in emitted if e == EVENT_GOVERNANCE_ENFORCED]
    assert len(enforced_events) == 1
    _, payload = enforced_events[0]
    assert payload["reason"] == "nav_discrepancy"
    assert payload["persisted_nav"] == pytest.approx(100_000.0)
    assert payload["broker_nav"] == pytest.approx(120_000.0)
    assert payload["discrepancy_pct"] > 1.0


def test_no_governance_event_on_small_discrepancy(tmp_path: Path) -> None:
    """Discrepancy ≤ 1% → no EVENT_GOVERNANCE_ENFORCED emitted."""
    from octa.execution.runner import run_execution
    from octa.core.governance.governance_audit import EVENT_GOVERNANCE_ENFORCED

    cfg = _make_run_cfg(tmp_path)
    # 0.5% discrepancy — within threshold
    CapitalState(nav=100_000.0, timestamp_utc="2026-01-01T00:00:00Z", source="broker").save(cfg.state_dir)

    emitted: list = []

    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
        patch("octa.execution.runner.GovernanceAudit") as MockAudit,
    ):
        mock_audit_instance = MagicMock()
        MockAudit.return_value = mock_audit_instance
        mock_audit_instance.emit.side_effect = lambda evt, payload: emitted.append((evt, payload))

        MockRouter.return_value = _stub_broker(nav=100_500.0)  # 0.5% higher
        run_execution(cfg)

    enforced_events = [e for e, _ in emitted if e == EVENT_GOVERNANCE_ENFORCED]
    assert len(enforced_events) == 0


def test_conservative_nav_used_when_persisted_higher(tmp_path: Path) -> None:
    """When persisted NAV > broker NAV, the higher (persisted) value is used."""
    from octa.execution.runner import run_execution

    cfg = _make_run_cfg(tmp_path)
    # Persisted NAV is higher (e.g. after an unreported loss)
    CapitalState(nav=150_000.0, timestamp_utc="2026-01-01T00:00:00Z", source="broker").save(cfg.state_dir)

    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
    ):
        MockRouter.return_value = _stub_broker(nav=100_000.0)
        run_execution(cfg)

    # The updated capital state should reflect the conservative (max) nav
    final_state = CapitalState.load_or_init(cfg.state_dir)
    assert final_state.nav == pytest.approx(150_000.0)
