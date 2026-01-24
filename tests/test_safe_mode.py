import pytest

from octa_ops.safe_mode import SafeModeManager


def test_halt_blocks_entries():
    sm = SafeModeManager()
    sm.set_halt(True, actor="ops", reason="test")
    with pytest.raises(RuntimeError):
        sm.execute_trade("BTC", 10, trade_type="entry", actor="strategy")


def test_exits_allowed_under_constraints():
    sm = SafeModeManager(initial_positions={"BTC": 100.0})
    sm.set_halt(True, actor="ops", reason="test")
    # allowed: reduces exposure
    sm.execute_trade("BTC", -50.0, trade_type="exit", actor="ops")
    assert sm.positions["BTC"] == 50.0

    # blocked: exit that would increase absolute exposure (e.g., selling more short)
    with pytest.raises(RuntimeError):
        sm.execute_trade("BTC", -200.0, trade_type="exit", actor="ops")

    # allowed if risk_approved True even if would increase exposure
    sm.execute_trade("BTC", -200.0, trade_type="exit", actor="ops", risk_approved=True)
