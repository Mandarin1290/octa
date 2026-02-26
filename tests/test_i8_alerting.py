"""I8: Alerting / Dashboard triggers — severity classification and dedup gating.

Tests that:
- _alert_severity() correctly classifies events as CRITICAL or WARNING.
- WARNING alerts within the dedup window are suppressed.
- CRITICAL alerts always bypass the dedup window (critical_always_send=True).
- When critical_always_send=False, CRITICAL alerts are also subject to dedup.
- The dedup window expires and the next alert goes through.
- Suppressed alerts are still written to the JSONL evidence file.
- emit_alert() records _alert_severity field in the evidence row.
- Telegram message includes [SEVERITY][event_type] prefix.
- runner.py emits a CRITICAL alert before raising TWS_PROBE_FAILED.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from octa.execution.notifier import ExecutionNotifier, _alert_severity


# ── _alert_severity() unit tests ──────────────────────────────────────────────

@pytest.mark.parametrize(
    "event_type, payload, expected",
    [
        ("MODEL_LOAD_REJECTED", {}, "CRITICAL"),
        ("DAILY_LOSS_LIMIT", {}, "CRITICAL"),
        ("DRAWDOWN_LIMIT", {}, "CRITICAL"),
        ("GOVERNANCE_ENFORCED", {"reason": "tws_not_ready"}, "CRITICAL"),
        ("GOVERNANCE_ENFORCED", {"reason": "drift_rollback"}, "WARNING"),
        ("GOVERNANCE_ENFORCED", {"reason": "nav_discrepancy"}, "WARNING"),
        ("GOVERNANCE_ENFORCED", {}, "WARNING"),
        ("drift_breach_warning", {}, "WARNING"),
        ("risk_block", {}, "WARNING"),
        ("nav_reconcile_warning", {}, "WARNING"),
        ("unknown_event", {}, "WARNING"),
    ],
)
def test_alert_severity_classification(event_type: str, payload: dict, expected: str) -> None:
    assert _alert_severity(event_type, payload) == expected


# ── emit_alert() dedup and severity tests ─────────────────────────────────────

def _make_notifier(tmp_path: Path, dedup_window: int = 300) -> ExecutionNotifier:
    n = ExecutionNotifier(
        evidence_dir=tmp_path / "notifications",
        alert_dedup_window_seconds=dedup_window,
        critical_always_send=True,
    )
    # Disable actual Telegram; _send_telegram_raw returns (False, "telegram_disabled")
    return n


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_warning_alert_first_call_goes_through(tmp_path: Path) -> None:
    n = _make_notifier(tmp_path)
    result = n.emit_alert("drift_breach_warning", {"model": "AAPL_1D"})
    rows = _read_jsonl(n.notifications_path)
    assert len(rows) == 1
    assert rows[0]["error"] != "alert_dedup_suppressed"


def test_warning_alert_dedup_suppressed_within_window(tmp_path: Path) -> None:
    n = _make_notifier(tmp_path, dedup_window=300)
    n.emit_alert("drift_breach_warning", {"model": "AAPL_1D"})
    n.emit_alert("drift_breach_warning", {"model": "AAPL_1D"})  # duplicate

    rows = _read_jsonl(n.notifications_path)
    assert len(rows) == 2
    # Second row is suppressed
    assert rows[1]["error"] == "alert_dedup_suppressed"


def test_critical_alert_bypasses_dedup(tmp_path: Path) -> None:
    n = _make_notifier(tmp_path, dedup_window=300)
    n.emit_alert("GOVERNANCE_ENFORCED", {"reason": "tws_not_ready"})
    n.emit_alert("GOVERNANCE_ENFORCED", {"reason": "tws_not_ready"})  # same event_type

    rows = _read_jsonl(n.notifications_path)
    assert len(rows) == 2
    # Second CRITICAL row must NOT be suppressed
    assert rows[1]["error"] != "alert_dedup_suppressed"


def test_critical_subject_to_dedup_when_flag_false(tmp_path: Path) -> None:
    n = ExecutionNotifier(
        evidence_dir=tmp_path / "notifications",
        alert_dedup_window_seconds=300,
        critical_always_send=False,  # dedup applies to everything
    )
    n.emit_alert("GOVERNANCE_ENFORCED", {"reason": "tws_not_ready"})
    n.emit_alert("GOVERNANCE_ENFORCED", {"reason": "tws_not_ready"})

    rows = _read_jsonl(n.notifications_path)
    assert rows[1]["error"] == "alert_dedup_suppressed"


def test_dedup_expires_after_window(tmp_path: Path) -> None:
    n = _make_notifier(tmp_path, dedup_window=0)  # 0-second window → no suppression
    n.emit_alert("drift_breach_warning", {"x": 1})
    n.emit_alert("drift_breach_warning", {"x": 2})

    rows = _read_jsonl(n.notifications_path)
    assert len(rows) == 2
    assert all(r["error"] != "alert_dedup_suppressed" for r in rows)


def test_dedup_is_per_event_type_not_payload(tmp_path: Path) -> None:
    n = _make_notifier(tmp_path, dedup_window=300)
    n.emit_alert("drift_breach_warning", {"model": "AAPL_1D"})
    n.emit_alert("risk_block", {"model": "AAPL_1D"})  # different event_type, same payload

    rows = _read_jsonl(n.notifications_path)
    assert len(rows) == 2
    assert all(r["error"] != "alert_dedup_suppressed" for r in rows)


def test_suppressed_alert_still_written_to_jsonl(tmp_path: Path) -> None:
    n = _make_notifier(tmp_path, dedup_window=300)
    n.emit_alert("risk_block", {"symbol": "AAPL"})
    n.emit_alert("risk_block", {"symbol": "AAPL"})  # suppressed

    rows = _read_jsonl(n.notifications_path)
    assert len(rows) == 2
    suppressed = [r for r in rows if r["error"] == "alert_dedup_suppressed"]
    assert len(suppressed) == 1


def test_emit_alert_records_severity_in_jsonl(tmp_path: Path) -> None:
    n = _make_notifier(tmp_path)
    n.emit_alert("MODEL_LOAD_REJECTED", {"model_key": "AAPL_1D", "reason": "drift_disabled"})

    rows = _read_jsonl(n.notifications_path)
    assert rows[0]["payload"]["_alert_severity"] == "CRITICAL"


def test_emit_alert_warning_records_warning_severity(tmp_path: Path) -> None:
    n = _make_notifier(tmp_path)
    n.emit_alert("drift_breach_warning", {"breach_count": 2})

    rows = _read_jsonl(n.notifications_path)
    assert rows[0]["payload"]["_alert_severity"] == "WARNING"


def test_telegram_message_has_severity_prefix(tmp_path: Path) -> None:
    """emit_alert() composes [SEVERITY][event_type] prefix in the Telegram text."""
    n = _make_notifier(tmp_path)
    sent_texts: list[str] = []

    def _fake_raw(text: str):
        sent_texts.append(text)
        return False, "telegram_disabled"

    n._send_telegram_raw = _fake_raw
    n.emit_alert("drift_breach_warning", {"model": "AAPL_1D"})

    assert len(sent_texts) == 1
    assert sent_texts[0].startswith("[WARNING][drift_breach_warning]")


def test_telegram_critical_message_prefix(tmp_path: Path) -> None:
    n = _make_notifier(tmp_path)
    sent_texts: list[str] = []

    def _fake_raw(text: str):
        sent_texts.append(text)
        return False, "telegram_disabled"

    n._send_telegram_raw = _fake_raw
    n.emit_alert("MODEL_LOAD_REJECTED", {"model_key": "AAPL_1D"})

    assert sent_texts[0].startswith("[CRITICAL][MODEL_LOAD_REJECTED]")


def test_emit_alert_does_not_affect_emit_dedup(tmp_path: Path) -> None:
    """emit() and emit_alert() maintain separate dedup state."""
    n = _make_notifier(tmp_path, dedup_window=300)
    n.emit_alert("risk_block", {"symbol": "AAPL"})  # marks _last_alert_ts
    # emit() uses _last_event_ts keyed by full payload — independent
    result = n.emit("risk_block", {"symbol": "AAPL"})
    rows = _read_jsonl(n.notifications_path)
    # emit() row should NOT be alert_dedup_suppressed (different state)
    emit_rows = [r for r in rows if r["error"] != "alert_dedup_suppressed"]
    assert any(r["type"] == "risk_block" for r in emit_rows)


# ── runner integration test ───────────────────────────────────────────────────

def test_tws_probe_failure_triggers_critical_alert(tmp_path: Path) -> None:
    """Runner calls notifier.emit_alert with GOVERNANCE_ENFORCED/tws_not_ready on probe failure."""
    from octa.execution.runner import run_execution, ExecutionConfig

    drift_dir = tmp_path / "drift"
    drift_dir.mkdir()
    cfg = ExecutionConfig(
        mode="paper",
        evidence_dir=tmp_path / "evidence",
        base_evidence_dir=tmp_path / "base_evidence",
        state_dir=tmp_path / "state",
        drift_registry_dir=drift_dir,
        tws_probe_timeout_sec=5,
    )

    alert_calls: list[tuple] = []

    class SpyNotifier(ExecutionNotifier):
        def emit_alert(self, event_type, payload):
            alert_calls.append((event_type, payload))
            return super().emit_alert(event_type, payload)

    with (
        patch("octa.execution.runner.BrokerRouter") as MockRouter,
        patch("octa.execution.runner.build_ml_selection", return_value={"eligible_rows": []}),
        patch("octa.execution.runner.tws_probe", return_value=False),
        patch("octa.execution.runner.ExecutionNotifier", SpyNotifier),
    ):
        broker = MagicMock()
        broker.account_snapshot.return_value = {"net_liquidation": 120_000.0, "currency": "USD"}
        MockRouter.return_value = broker

        with pytest.raises(RuntimeError, match="TWS_PROBE_FAILED"):
            run_execution(cfg)

    # Must have emitted a CRITICAL alert for tws_not_ready
    assert any(
        evt == "GOVERNANCE_ENFORCED" and p.get("reason") == "tws_not_ready"
        for evt, p in alert_calls
    ), f"Expected tws_not_ready alert, got: {alert_calls}"
