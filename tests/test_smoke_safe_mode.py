from octa_core.broker_adapter import BrokerAdapter
from octa_ops.broker_failover import BrokerFailoverManager, BrokerHealthMonitor
from octa_ops.safe_mode import SafeModeManager
from octa_sentinel.kill_switch import KillSwitch


def test_end_to_end_safe_mode_and_failover_simulation():
    # setup
    sm = SafeModeManager()
    sm.set_halt(False, actor="ops")

    ks = KillSwitch(operator_keys={"a": "k1", "b": "k2"})

    monitor = BrokerHealthMonitor()
    monitor.register("primary")
    monitor.register("secondary")
    manager = BrokerFailoverManager(monitor)

    # ensure adapter simulates orders
    adapter = BrokerAdapter()
    manager.set_adapter(adapter)

    # place an order via manager
    manager.place_order("primary", "smoke1", "EURUSD", 100.0, "buy")
    assert "smoke1" in manager.registry or any(
        o.get("client_order_id") for o in monitor.brokers["primary"].orders.values()
    )

    # trigger kill switch and ensure safe mode blocks new entries
    ks.trigger(reason="drill")
    sm.set_halt(True, actor="ops", reason="drill")
    ok, reason = sm.allow_trade("EURUSD", delta=1.0, trade_type="entry")
    assert not ok and "global_halt" in reason
