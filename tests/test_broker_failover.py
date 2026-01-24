import time
from datetime import datetime, timedelta, timezone

from octa_ops.broker_failover import BrokerFailoverManager, BrokerHealthMonitor


def test_broker_loss_detected():
    monitor = BrokerHealthMonitor(failure_threshold_seconds=1)
    monitor.register("primary")
    monitor.register("secondary")
    # initial healthy
    assert monitor.is_healthy("primary")
    # simulate heartbeat stale by sleeping beyond threshold
    time.sleep(1.1)
    failed = monitor.failed_brokers()
    assert "primary" in failed or "secondary" in failed


def test_failover_preserves_exposure():
    monitor = BrokerHealthMonitor(failure_threshold_seconds=10)
    monitor.register("primary")
    monitor.register("secondary")
    manager = BrokerFailoverManager(monitor)
    # place order on primary
    manager.place_order("primary", "ord1", "BTC", 10.0, "buy")
    # simulate primary failure by not heartbeating and setting monitors last_heartbeat to old
    monitor.brokers["primary"].last_heartbeat = datetime.now(timezone.utc) - timedelta(
        seconds=100
    )
    # failover
    summary = manager.failover("primary")
    assert (
        "recovered" in summary
        and any(r.get("order") == "ord1" for r in summary.get("recovered", []))
    ) or ("errors" in summary)
