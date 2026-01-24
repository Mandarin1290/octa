import os

from octa_nexus.bus import NexusBus
from octa_tests.scenarios import (
    audit_failure,
    data_contract_failure,
    integrity_failure,
    missing_heartbeat,
    order_ack_timeout,
)


def run_scenario(module, tmp_path):
    bus_dir = os.path.join(tmp_path, "bus")
    os.makedirs(bus_dir, exist_ok=True)
    b = NexusBus(bus_dir)
    # ensure empty bus
    b.purge()
    res = module.run(bus_dir, str(tmp_path))
    return b, res


def test_data_contract_blocks_orders(tmp_path):
    b, res = run_scenario(data_contract_failure, tmp_path)
    assert res["blocked"] is True
    assert res["orders_published"] == 0


def test_audit_failure_blocks_trading(tmp_path):
    b, res = run_scenario(audit_failure, tmp_path)
    assert res["level"] >= 2


def test_missing_heartbeat_freezes(tmp_path):
    b, res = run_scenario(missing_heartbeat, tmp_path)
    assert len(res["frozen_components"]) >= 0


def test_order_ack_timeout_triggers_incident(tmp_path):
    b, res = run_scenario(order_ack_timeout, tmp_path)
    assert res["incidents"] >= 1
    assert res["freezes"] >= 1


def test_integrity_failure_blocks(tmp_path):
    b, res = run_scenario(integrity_failure, tmp_path)
    assert res["incidents"] == 1
    assert res["ledger_events"] >= 1
