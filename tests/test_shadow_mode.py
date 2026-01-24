from octa_nexus.shadow_runtime import ShadowRuntime
from octa_vertex.shadow_executor import ShadowExecutor


class DummyBroker:
    def __init__(self):
        self.calls = []

    def submit_order(self, order):
        self.calls.append(order)
        return {"status": "SENT"}


class DummyAllocator:
    def pre_trade_check(self, order):
        return True, "ok"


class DummySentinel:
    def __init__(self):
        self._level = 0
        self.last = None

    def set_gate(self, level, reason):
        self._level = level
        self.last = (level, reason)

    def get_gate_level(self):
        return self._level


class PriceProvider:
    def __init__(self, prices):
        self.prices = prices

    def get_price(self, instr):
        return self.prices.get(instr)


def test_zero_net_exposure(tmp_path):
    broker = DummyBroker()
    allocator = DummyAllocator()
    sentinel = DummySentinel()
    prices = {"AAPL": 100.0}
    pp = PriceProvider(prices)
    ShadowRuntime({"shadow_mode": True})

    calls = []

    def audit_fn(event, payload):
        calls.append((event, payload))

    cfg = {"shadow_mode": True}
    exec = ShadowExecutor(broker, allocator, sentinel, pp, audit_fn, cfg)

    order = {
        "order_id": "s1",
        "instrument": "AAPL",
        "qty": 10,
        "side": "BUY",
        "order_type": "MKT",
    }
    res = exec.submit_order(order)
    assert res["status"] == "FILLED"
    # ensure broker was NOT called
    assert len(broker.calls) == 0
    # ensure shadow position updated but real exposure unchanged (we don't touch broker)
    assert exec.shadow_positions.get("AAPL") == 10


def test_kill_switch_blocks_shadow_orders():
    broker = DummyBroker()
    allocator = DummyAllocator()
    sentinel = DummySentinel()
    prices = {"FUT_ES": 4000.0}
    pp = PriceProvider(prices)

    def audit_fn(e, p):
        pass

    cfg = {"shadow_mode": True, "kill_threshold": 2}
    exec = ShadowExecutor(broker, allocator, sentinel, pp, audit_fn, cfg)

    # simulate sentinel kill
    sentinel.set_gate(3, "manual_kill")

    order = {
        "order_id": "k1",
        "instrument": "FUT_ES",
        "qty": 1,
        "side": "SELL",
        "order_type": "MKT",
    }
    res = exec.submit_order(order)
    assert res["status"] == "REJECTED"
    assert res["reason"] == "kill-switch"
