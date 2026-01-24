from datetime import datetime, timedelta

from octa_accounting.cost_monitor import CostMonitor


def iso(t):
    return t.isoformat()


def test_cost_drift_detected_and_alerted():
    mon = CostMonitor(
        long_window=80,
        short_window=20,
        min_samples=50,
        drift_threshold=0.15,
        max_cost_increase_for_action=0.25,
        escalate_count=1,
    )
    strat = "S-A"
    mon.set_capacity(strat, 2_000_000.0)
    now = datetime.utcnow()

    # baseline stable costs
    for i in range(60):
        t = now - timedelta(minutes=60 - i)
        mon.record_costs(
            iso(t),
            strat,
            execution=100.0,
            financing=10.0,
            infrastructure=50.0,
            slippage_delta=1.0,
        )

    # recent drift: increases in execution and infra and slippage
    for i in range(30):
        t = now + timedelta(minutes=i)
        mon.record_costs(
            iso(t),
            strat,
            execution=120.0 + i * 0.5,
            financing=12.0,
            infrastructure=60.0 + i * 0.2,
            slippage_delta=1.5 + i * 0.01,
        )

    alert = mon.evaluate(strat)
    assert alert is not None
    assert alert.evidence_hash
    assert alert.relative_increase > 0.0
    # suggested action may be set when current capacity exists
    assert alert.suggested_action in (None, "reduce_capacity")


def test_no_false_positive_for_stable_costs():
    mon = CostMonitor(
        long_window=80, short_window=20, min_samples=50, drift_threshold=0.15
    )
    strat = "S-B"
    now = datetime.utcnow()
    for i in range(120):
        t = now + timedelta(minutes=i)
        mon.record_costs(
            iso(t),
            strat,
            execution=90.0 + (i % 5) * 0.2,
            financing=9.0 + (i % 3) * 0.05,
            infrastructure=45.0 + (i % 4) * 0.1,
            slippage_delta=0.9 + (i % 7) * 0.01,
        )

    alert = mon.evaluate(strat)
    assert alert is None
