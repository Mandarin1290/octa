from datetime import datetime, timedelta

import pytest

from octa_capital.capacity_erosion import CapacityErosionMonitor


def _iso(dt):
    return dt.isoformat()


def test_erosion_detected_and_capacity_adjusted():
    monitor = CapacityErosionMonitor(
        long_window=80,
        short_window=20,
        min_samples=50,
        slippage_increase_pct=0.2,
        impact_increase_pct=0.2,
        fill_drop_pct=0.15,
        max_reduction=0.5,
        escalate_count=1,
    )
    strategy = "strat-A"
    # set an initial capacity
    monitor.set_capacity(strategy, 1_000_000.0)

    # create baseline stable metrics
    now = datetime.utcnow()
    for i in range(60):
        t = now - timedelta(minutes=60 - i)
        monitor.record_metrics(
            _iso(t), strategy, slippage=0.10, impact=0.02, fill_ratio=0.95
        )

    # recent degradation: slippage and impact increase, fill_ratio drops
    for i in range(30):
        t = now + timedelta(minutes=i)
        monitor.record_metrics(
            _iso(t),
            strategy,
            slippage=0.14 + i * 0.001,
            impact=0.03 + i * 0.0008,
            fill_ratio=0.90 - i * 0.002,
        )

    alert = monitor.evaluate(strategy)
    assert alert is not None, "Erosion should be detected"
    assert alert.evidence_hash, "Alert must contain an evidence hash"
    suggested = alert.suggested_capacity
    assert suggested is not None and suggested < 1_000_000.0, (
        "Suggested capacity should be reduced"
    )

    # apply suggested capacity and verify stored
    monitor.set_capacity(strategy, suggested)
    assert pytest.approx(monitor.get_capacity(strategy), rel=1e-9) == suggested


def test_no_false_positive_under_stable_conditions():
    monitor = CapacityErosionMonitor(
        long_window=80,
        short_window=20,
        min_samples=50,
        slippage_increase_pct=0.2,
        impact_increase_pct=0.2,
        fill_drop_pct=0.15,
        max_reduction=0.5,
    )
    strategy = "strat-B"
    now = datetime.utcnow()
    for i in range(120):
        t = now + timedelta(minutes=i)
        monitor.record_metrics(
            _iso(t),
            strategy,
            slippage=0.08 + (i % 5) * 0.0005,
            impact=0.015 + (i % 3) * 0.0004,
            fill_ratio=0.96 - (i % 7) * 0.0003,
        )

    alert = monitor.evaluate(strategy)
    assert alert is None, "Stable signals should not trigger erosion"
