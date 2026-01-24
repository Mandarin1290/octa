from octa_ops.recovery import RecoveryManager


def test_mismatch_detected_and_resolved():
    internal = {"A": 100.0, "B": 50.0}
    broker = {"A": 90.0, "B": 50.0, "C": 10.0}
    mgr = RecoveryManager(internal_positions=internal)

    # Start reconciliation but do not auto-resolve -> recovery mode should block trading
    res = mgr.reconcile_with_broker(broker, actor="system", auto_resolve=False)
    assert res["resolved"] is False
    assert len(res["mismatches"]) >= 1
    assert mgr.in_recovery is True
    assert mgr.allow_trading() is False

    # Now resolve mismatches and complete recovery
    mgr.resolve_mismatches(broker, actor="ops")
    mgr.complete_recovery(actor="ops")
    assert mgr.in_recovery is False
    assert mgr.allow_trading() is True
    # internal should now equal broker exactly
    assert mgr.internal_positions == broker


def test_recovery_enforced_on_resolution_failure():
    internal = {"A": 10.0}
    # broker snapshot contains invalid negative position -> resolution must fail
    broker = {"A": -5.0}
    mgr = RecoveryManager(internal_positions=internal)

    res = mgr.reconcile_with_broker(broker, actor="system", auto_resolve=True)
    # resolution should have failed and recovery should be active
    assert res["resolved"] is False
    assert mgr.in_recovery is True
    assert mgr.allow_trading() is False
