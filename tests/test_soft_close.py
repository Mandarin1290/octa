from octa_capital.aum_state import AUMState
from octa_capital.soft_close import SoftCloseEngine
from octa_ledger.core import AuditChain


def test_trigger_activates_soft_close_and_blocks_inflow():
    ledger = AuditChain()
    aum = AUMState(
        audit_fn=lambda e, p: ledger.append({"event": e, **p}),
        initial_internal=1000.0,
        initial_external=0.0,
    )
    engine = SoftCloseEngine(
        thresholds={
            "capacity_utilization": 0.8,
            "slippage_delta": 0.3,
            "correlation_index": 0.7,
        },
        audit_fn=lambda e, p: ledger.append({"event": e, **p}),
    )
    engine.attach(aum)

    # initially inactive
    assert engine.active is False

    # trigger by capacity utilization
    engine.check_and_update(
        capacity_utilization=0.85, slippage_delta=0.0, correlation_index=0.0
    )
    assert engine.active is True

    # attempt external inflow should be blocked
    prev_external = aum._external
    res = aum.inflow(100.0, source="external", reason="test")
    assert res is False
    assert aum._external == prev_external

    # internal PnL should still apply
    aum.apply_pnl(10.0, reason="pnl")
    assert aum._internal == 1010.0

    # lift soft close and allow inflow
    engine.lift(reason="ok")
    assert engine.active is False
    aum.inflow(100.0, source="external", reason="test")
    # after lift, inflow should succeed (returns None) and update external
    assert aum._external == prev_external + 100.0


def test_slippage_and_correlation_triggers():
    aum = AUMState(initial_internal=500.0, initial_external=0.0)
    engine = SoftCloseEngine(
        thresholds={
            "capacity_utilization": 0.9,
            "slippage_delta": 0.1,
            "correlation_index": 0.2,
        }
    )
    engine.attach(aum)

    engine.check_and_update(
        capacity_utilization=0.0, slippage_delta=0.2, correlation_index=0.0
    )
    assert engine.active is True

    engine.lift()
    engine.check_and_update(
        capacity_utilization=0.0, slippage_delta=0.0, correlation_index=0.3
    )
    assert engine.active is True
