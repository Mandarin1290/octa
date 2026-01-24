from datetime import datetime, timedelta, timezone

from octa_ops.detection import DetectionEngine
from octa_ops.incidents import IncidentManager


def iso_now(offset_seconds=0):
    return (datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).isoformat()


def test_correlated_signals_escalate():
    im = IncidentManager()
    de = DetectionEngine(im, window_seconds=60)

    # emit three latency spikes (weak individually) within window
    de.ingest(component="trade_engine", signal_type="latency_spike", ts=iso_now(-10))
    de.ingest(component="trade_engine", signal_type="latency_spike", ts=iso_now(-8))
    de.ingest(component="trade_engine", signal_type="latency_spike", ts=iso_now(-5))

    incidents = de.evaluate()
    assert len(incidents) == 1
    # classification should be S2 (since 3*5=15 -> >=11)
    assert incidents[0]["severity"].startswith("S")


def test_noise_ignored_single_weak_signal():
    im = IncidentManager()
    de = DetectionEngine(im, window_seconds=60)
    # single weak signal
    de.ingest(component="trade_engine", signal_type="latency_spike", ts=iso_now())
    incidents = de.evaluate()
    assert len(incidents) == 0
