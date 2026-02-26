"""I7: TWS Readiness Verification — startup probe before paper/live execution.

Tests that:
- tws_probe() returns True when health_check ok=True and account_snapshot is a dict.
- tws_probe() returns False when health_check ok=False.
- tws_probe() returns False when an exception is raised by the broker.
- tws_probe() returns False when the probe times out.
- paper/live modes raise RuntimeError when the probe fails.
- shadow/dry-run modes skip the probe entirely.
- A failed probe writes tws_probe_failed.json evidence.
- A failed probe emits EVENT_GOVERNANCE_ENFORCED with reason=tws_not_ready.
"""
from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from octa.execution.tws_probe import tws_probe


# ── tws_probe() unit tests ────────────────────────────────────────────────────

def _healthy_broker() -> MagicMock:
    broker = MagicMock()
    broker.health_check.return_value = {"ok": True, "mode": "paper"}
    broker.account_snapshot.return_value = {"net_liquidation": 100_000.0, "currency": "USD"}
    return broker


def test_probe_returns_true_when_healthy() -> None:
    assert tws_probe(_healthy_broker()) is True


def test_probe_returns_false_when_health_not_ok() -> None:
    broker = MagicMock()
    broker.health_check.return_value = {"ok": False, "reason": "tws_disconnected"}
    assert tws_probe(broker) is False


def test_probe_returns_false_when_health_raises() -> None:
    broker = MagicMock()
    broker.health_check.side_effect = ConnectionError("TWS unreachable")
    assert tws_probe(broker) is False


def test_probe_returns_false_when_snapshot_raises() -> None:
    broker = MagicMock()
    broker.health_check.return_value = {"ok": True}
    broker.account_snapshot.side_effect = RuntimeError("snapshot timeout")
    assert tws_probe(broker) is False


def test_probe_returns_false_when_snapshot_is_not_dict() -> None:
    broker = MagicMock()
    broker.health_check.return_value = {"ok": True}
    broker.account_snapshot.return_value = None  # not a dict
    assert tws_probe(broker) is False


def test_probe_returns_false_on_timeout() -> None:
    broker = MagicMock()
    broker.health_check.return_value = {"ok": True}

    def _slow_snapshot():
        time.sleep(5)
        return {"nav": 100_000.0}

    broker.account_snapshot.side_effect = _slow_snapshot
    result = tws_probe(broker, timeout_seconds=0)  # zero timeout → immediate expiry
    assert result is False


def test_probe_missing_ok_key_treated_as_not_ok() -> None:
    """health_check() returning {} (no 'ok' key) → probe fails."""
    broker = MagicMock()
    broker.health_check.return_value = {}  # ok key missing → falsy
    assert tws_probe(broker) is False


# ── runner integration tests ──────────────────────────────────────────────────

def _make_cfg(tmp_path: Path, mode: str):
    from octa.execution.runner import ExecutionConfig
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir()
    return ExecutionConfig(
        mode=mode,
        evidence_dir=tmp_path / "evidence",
        base_evidence_dir=tmp_path / "base_evidence",
        state_dir=tmp_path / "state",
        drift_registry_dir=drift_dir,
        tws_probe_timeout_sec=5,
    )


def _stub_broker(*, healthy: bool = True, nav: float = 120_000.0) -> MagicMock:
    broker = MagicMock()
    broker.health_check.return_value = {"ok": healthy, "mode": "paper"}
    if healthy:
        broker.account_snapshot.return_value = {"net_liquidation": nav, "currency": "USD"}
    else:
        broker.account_snapshot.return_value = {}
    broker.place_order.return_value = {"status": "ok"}
    return broker


def test_probe_blocks_paper_execution_when_unhealthy(tmp_path: Path) -> None:
    """Paper mode + unhealthy probe → RuntimeError('TWS_PROBE_FAILED')."""
    from octa.execution.runner import run_execution

    cfg = _make_cfg(tmp_path, mode="paper")

    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
        patch("octa.execution.runner.tws_probe", return_value=False),
    ):
        MockRouter.return_value = _stub_broker(nav=120_000.0)
        with pytest.raises(RuntimeError, match="TWS_PROBE_FAILED"):
            run_execution(cfg)


def test_probe_blocks_live_execution_when_unhealthy(tmp_path: Path) -> None:
    """Live mode + unhealthy probe → RuntimeError('TWS_PROBE_FAILED')."""
    from octa.execution.runner import run_execution

    cfg = _make_cfg(tmp_path, mode="live")

    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
        patch("octa.execution.runner.tws_probe", return_value=False),
    ):
        MockRouter.return_value = _stub_broker(nav=120_000.0)
        with pytest.raises(RuntimeError, match="TWS_PROBE_FAILED"):
            run_execution(cfg)


def test_probe_skipped_for_shadow_mode(tmp_path: Path) -> None:
    """Shadow/dry-run mode skips the TWS probe entirely."""
    from octa.execution.runner import run_execution

    cfg = _make_cfg(tmp_path, mode="shadow")

    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
        patch("octa.execution.runner.tws_probe") as mock_probe,
    ):
        MockRouter.return_value = _stub_broker()
        run_execution(cfg)

    mock_probe.assert_not_called()


def test_probe_skipped_for_dry_run_mode(tmp_path: Path) -> None:
    """dry-run mode skips the TWS probe entirely."""
    from octa.execution.runner import run_execution

    cfg = _make_cfg(tmp_path, mode="dry-run")

    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
        patch("octa.execution.runner.tws_probe") as mock_probe,
    ):
        MockRouter.return_value = MagicMock()
        MockRouter.return_value.account_snapshot.return_value = {}
        run_execution(cfg)

    mock_probe.assert_not_called()


def test_probe_failure_writes_evidence_file(tmp_path: Path) -> None:
    """Failed probe writes tws_probe_failed.json to evidence dir."""
    from octa.execution.runner import run_execution

    cfg = _make_cfg(tmp_path, mode="paper")

    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
        patch("octa.execution.runner.tws_probe", return_value=False),
    ):
        MockRouter.return_value = _stub_broker(nav=120_000.0)
        with pytest.raises(RuntimeError):
            run_execution(cfg)

    evidence_file = cfg.evidence_dir / "tws_probe_failed.json"
    assert evidence_file.exists()

    import json
    data = json.loads(evidence_file.read_text())
    assert data["reason"] == "TWS_PROBE_FAILED"
    assert data["mode"] == "paper"


def test_probe_failure_emits_governance_event(tmp_path: Path) -> None:
    """Failed probe emits EVENT_GOVERNANCE_ENFORCED with reason=tws_not_ready."""
    from octa.execution.runner import run_execution
    from octa.core.governance.governance_audit import EVENT_GOVERNANCE_ENFORCED

    cfg = _make_cfg(tmp_path, mode="paper")
    emitted: list = []

    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
        patch("octa.execution.runner.tws_probe", return_value=False),
        patch("octa.execution.runner.GovernanceAudit") as MockAudit,
    ):
        mock_audit = MagicMock()
        MockAudit.return_value = mock_audit
        mock_audit.emit.side_effect = lambda evt, payload: emitted.append((evt, payload))

        MockRouter.return_value = _stub_broker(nav=120_000.0)
        with pytest.raises(RuntimeError):
            run_execution(cfg)

    enforced = [(e, p) for e, p in emitted if e == EVENT_GOVERNANCE_ENFORCED]
    assert any(p.get("reason") == "tws_not_ready" for _, p in enforced)


def test_healthy_probe_allows_paper_execution(tmp_path: Path) -> None:
    """Healthy probe → paper execution proceeds normally (no RuntimeError)."""
    from octa.execution.runner import run_execution

    cfg = _make_cfg(tmp_path, mode="paper")

    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
        patch("octa.execution.runner.tws_probe", return_value=True),
    ):
        MockRouter.return_value = _stub_broker(healthy=True, nav=120_000.0)
        result = run_execution(cfg)

    assert result["mode"] == "paper"
