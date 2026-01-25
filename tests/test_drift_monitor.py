from __future__ import annotations

from datetime import datetime, timedelta, timezone

from octa_ledger.events import AuditEvent
from octa_ledger.store import LedgerStore

from octa.core.governance.drift_monitor import evaluate_drift


def test_drift_monitor_disables_on_breach(tmp_path) -> None:
    ledger_dir = tmp_path / "ledger"
    ledger = LedgerStore(str(ledger_dir))
    start = datetime.now(timezone.utc) - timedelta(days=30)
    nav = 100.0
    for i in range(25):
        nav *= 0.999  # mild decay
        ts = (start + timedelta(days=i)).isoformat()
        ev = AuditEvent.create(actor="test", action="performance.nav", payload={"date": ts, "nav": nav}, severity="INFO")
        ledger.append(ev)

    decision = evaluate_drift(
        ledger_dir=str(ledger_dir),
        model_key="ABC_1D",
        gate="global_1d",
        timeframe="1D",
        bucket="default",
        cfg={"kpi_threshold": 0.0, "window_days": 20, "breach_days": 2},
    )
    decision = evaluate_drift(
        ledger_dir=str(ledger_dir),
        model_key="ABC_1D",
        gate="global_1d",
        timeframe="1D",
        bucket="default",
        cfg={"kpi_threshold": 0.0, "window_days": 20, "breach_days": 2},
    )
    assert decision.disabled is True
