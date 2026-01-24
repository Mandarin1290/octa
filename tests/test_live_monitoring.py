from octa_monitoring.live_capital_monitor import LiveCapitalMonitor


def test_nav_drift_alert_and_escalation():
    m = LiveCapitalMonitor(nav_drift_threshold=0.05, escalate_count=2)
    # first large drift triggers warning
    alerts = m.record_nav(100.0, 93.0)  # 7% drift
    assert len(alerts) == 1
    assert alerts[0].metric == "nav_drift"
    assert alerts[0].severity == "warning"

    # second similar trigger should escalate to critical (escalate_count=2)
    alerts2 = m.record_nav(100.0, 92.0)
    assert len(alerts2) == 1
    assert alerts2[0].severity == "critical"


def test_fee_accrual_threshold():
    m = LiveCapitalMonitor(fee_accrual_threshold=5000.0)
    no_alert = m.record_fee_accrual(100.0)
    assert no_alert == []
    alerts = m.record_fee_accrual(6000.0)
    assert len(alerts) == 1
    assert alerts[0].metric == "fee_accrual"


def test_exposure_vs_capital_threshold():
    m = LiveCapitalMonitor(exposure_percent_threshold=0.3)
    # exposure 200 on capital 1000 => 0.2 -> no alert
    assert m.record_exposure(200.0, 1000.0) == []
    # exposure 400 on capital 1000 => 0.4 -> alert
    alerts = m.record_exposure(400.0, 1000.0)
    assert len(alerts) == 1
    assert alerts[0].metric == "exposure_vs_capital"
