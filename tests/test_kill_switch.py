from octa_core.kill_switch import kill_switch
from octa_wargames.execution_abuse import OrderManagementSystem


def test_instant_flattening():
    oms = OrderManagementSystem()
    # set positions for two strategies
    oms._positions["s1"] = {"AAA": 10.0, "BBB": 5.0}
    oms._positions["s2"] = {"AAA": 2.0}

    engaged = kill_switch.engage("ops", "drill", automated=False)
    assert engaged is True
    assert kill_switch.is_engaged() is True

    kill_switch.flatten_portfolio(oms)

    # positions should be zeroed
    assert all(all(q == 0.0 for q in pos.values()) for pos in oms._positions.values())


def test_no_reentry_without_clearance():
    # start fresh
    ks = kill_switch
    # ensure engaged state
    ks.engage("ops", "emergency", automated=True)
    # attempt to clear with insufficient role
    ok = ks.clear("trader", "bob")
    assert ok is False
    assert ks.is_engaged() is True

    # now clear with admin role
    ok2 = ks.clear("admin", "alice")
    assert ok2 is True
    assert ks.is_engaged() is False


from octa_sentinel.kill_switch import get_kill_switch
from octa_vertex.pretrade_regulatory import PreTradeRegulator
from octa_vertex.shadow_executor import ShadowExecutor


class DummyBroker:
    def submit_order(self, order):
        return {"status": "SENT"}


class DummyAllocator:
    def pre_trade_check(self, order):
        return True, "ok"


class DummySentinel:
    def __init__(self):
        self._level = 0

    def get_gate_level(self):
        return self._level

    def set_gate(self, level, reason):
        self._level = level


class PriceProvider:
    def get_price(self, instr):
        return 100.0


def test_kill_blocks_everything():
    ks = get_kill_switch()
    # trigger the global kill
    ks.trigger(source="test", reason="unit")

    sentinel = DummySentinel()
    regulator = PreTradeRegulator(
        config={}, sentinel_api=sentinel, audit_fn=lambda e, p: None
    )
    allowed, reason = regulator.pre_trade_check(
        {"instrument": "AAPL", "qty": 1, "side": "BUY"}
    )
    assert not allowed
    assert reason == "kill_switch"

    # shadow executor should also be blocked
    broker = DummyBroker()
    allocator = DummyAllocator()
    pp = PriceProvider()
    exec = ShadowExecutor(
        broker,
        allocator,
        sentinel,
        pp,
        audit_fn=lambda e, p: None,
        config={"shadow_mode": True},
    )
    res = exec.submit_order(
        {"order_id": "k1", "instrument": "AAPL", "qty": 1, "side": "BUY"}
    )
    assert res["status"] == "REJECTED"
    assert res["reason"] in ("kill-switch", "kill-check-error")


def test_manual_release_requires_dual_confirm():
    # create new kill switch with operator keys
    ks = get_kill_switch()
    ks.arm(reason="reset")
    ks.lock(source="admin", reason="maintenance")

    # configure operator keys
    ks.operator_keys["op1"] = "key1"
    ks.operator_keys["op2"] = "key2"

    # build payload signature
    ts = ks._now().isoformat()
    payload = f"manual_release|{ts}|unblock"
    sig1 = ks._sign(payload, "key1")
    sig2 = ks._sign(payload, "key2")

    ok = ks.manual_release("op1", sig1, "op2", sig2, reason="unblock", payload_ts=ts)
    assert ok is True
    assert ks.get_state().name == "ARMED"
