from octa_monitoring.longrun_stability import LongRunStabilityMonitor


def test_detect_memory_degradation_and_escalation():
    m = LongRunStabilityMonitor(
        long_window=20, short_window=5, ratio_threshold=1.2, escalate_count=2
    )
    # feed stable baseline
    for _ in range(20):
        m.record_memory(200 * 1024 * 1024)  # 200 MB
    # evaluate no alerts
    assert m.evaluate() == []

    # now gradual increase
    for _ in range(5):
        m.record_memory(260 * 1024 * 1024)  # 260 MB (~30% increase)
    alerts = m.evaluate()
    assert len(alerts) >= 1
    assert alerts[0].metric == "memory"
    assert alerts[0].severity == "warning"

    # trigger second time to escalate (use larger jump to ensure ratio stays above threshold)
    for _ in range(5):
        m.record_memory(300 * 1024 * 1024)
    alerts2 = m.evaluate()
    assert len(alerts2) >= 1
    assert alerts2[0].severity == "critical"


def test_latency_and_error_rate_detection():
    m = LongRunStabilityMonitor(
        long_window=10, short_window=3, ratio_threshold=1.15, escalate_count=3
    )
    # baseline latency 100ms
    for _ in range(10):
        m.record_latency(100.0)
    # small spike below threshold
    for _ in range(3):
        m.record_latency(110.0)
    assert m.evaluate() == []

    # sustained creep
    for _ in range(3):
        m.record_latency(140.0)
    alerts = m.evaluate()
    assert len(alerts) >= 1
    assert alerts[0].metric == "latency"

    # error-rate drift
    for _ in range(10):
        m.record_error(0.1)
    for _ in range(3):
        m.record_error(1.0)
    alerts2 = m.evaluate()
    assert any(a.metric == "error_rate" for a in alerts2)
