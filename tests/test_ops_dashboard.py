from octa_ops.broker_failover import BrokerHealthMonitor
from octa_ops.data_failures import DataFeedManager
from octa_ops.incidents import IncidentManager, Severity
from octa_ops.recovery import RecoveryManager
from octa_ops.safe_mode import SafeModeManager
from octa_reports.ops_dashboard import OpsDashboard


def test_dashboard_reconciles_with_ops_state():
    im = IncidentManager()
    inc = im.record_incident(
        title="P1", description="d", reporter="r", severity=Severity.S1
    )

    # brokers
    monitor = BrokerHealthMonitor(failure_threshold_seconds=1)
    monitor.register("b1")
    monitor.register("b2")

    # data feeds
    df = DataFeedManager(freshness_seconds=1, recovery_required=1)
    df.set_hierarchy("SYM", ["pfeed", "f1"])
    # report primary fresh
    df.report_update("pfeed", "SYM")

    # safe mode off
    sm = SafeModeManager(initial_positions={})

    # recovery manager no recovery
    rm = RecoveryManager(internal_positions={"SYM": 100.0})

    dash = OpsDashboard(
        incident_manager=im,
        broker_monitor=monitor,
        data_manager=df,
        safe_mode=sm,
        recovery_manager=rm,
    )
    snap = dash.snapshot()

    assert snap["trading_mode"] == "normal"
    assert snap["system_health"]["ok"] is True
    assert snap["active_incidents"][0]["id"] == inc.id
    # feed status should list SYM with primary fresh
    assert snap["feed_status"]["instruments"]["SYM"]["best_feed"] == "pfeed"
