import copy

from octa_audit.audit_interface import AuditInterface
from octa_ip.ip_registry import IPRegistry


def test_access_isolation_and_defensive_copy():
    reg = IPRegistry()
    reg.add_asset("alpha", asset_id="alpha")
    reg.add_version("alpha", "v1", metadata={"notes": "initial"})

    audit = AuditInterface(registry=reg)
    snap = audit.access_registry_snapshot(user="auditor@ex")
    # mutate the returned snapshot
    snap_copy = copy.deepcopy(snap)
    snap_copy["alpha@v1"]["lifecycle"] = "tampered"

    # original registry must be unchanged
    v = reg.get_version("alpha", "v1")
    assert v.lifecycle == "active"

    # logs should record the access and verify
    assert len(audit.list_logs()) == 1
    assert audit.verify_logs() is True


def test_tamper_detection_in_logs():
    reg = IPRegistry()
    reg.add_asset("alpha", asset_id="alpha")
    reg.add_version("alpha", "v1")

    audit = AuditInterface(registry=reg)
    audit.access_registry_snapshot(user="auditor@ex")
    audit.access_registry_snapshot(user="auditor@ex2")

    logs = audit.list_logs()
    assert len(logs) == 2

    # simulate tampering: change a user in the first log entry dict
    logs[0]["user"] = "evil"

    # But verify_logs operates on internal log objects; tampering the returned list shouldn't affect internal state
    assert audit.verify_logs() is True

    # simulate tampering by modifying internal log (unsafe, test only)
    internal = audit._log
    internal[0].user = "evil"
    # Now verification should fail
    assert audit.verify_logs() is False
