from octa_strategy.capacity import CapacityEngine


def test_capacity_cap_enforced():
    audit = []

    class SentinelMock:
        def __init__(self):
            self.calls = []

        def set_gate(self, level, reason):
            self.calls.append((level, reason))

    class AllocatorMock:
        def __init__(self):
            self.calls = []

        def allocate(self, strategy_id, amount):
            self.calls.append((strategy_id, amount))

    sentinel = SentinelMock()
    allocator = AllocatorMock()
    engine = CapacityEngine(
        audit_fn=lambda e, p: audit.append((e, p)),
        sentinel_api=sentinel,
        allocator_api=allocator,
    )

    # small capacity: adv small, high impact and high turnover
    engine.register_strategy(
        "S_CAP", adv=100.0, turnover=1.0, impact=0.5, adv_fraction=0.01, base_scaler=1.0
    )
    cap = engine.estimate_capacity("S_CAP")
    assert cap > 0

    # allocate up to capacity should succeed
    ok = engine.allocate("S_CAP", cap * 0.5)
    assert ok is True
    assert allocator.calls

    # allocation beyond capacity should be blocked
    ok2 = engine.allocate("S_CAP", cap)
    assert ok2 is False
    assert len(sentinel.calls) >= 1


def test_scaling_beyond_capacity_blocked():
    engine = CapacityEngine()
    engine.register_strategy(
        "S2", adv=200.0, turnover=0.5, impact=0.01, adv_fraction=0.01
    )
    cap = engine.estimate_capacity("S2")
    # try to allocate in many small chunks until over capacity
    allowed = 0
    i = 0
    while True:
        amt = cap * 0.2
        ok = engine.allocate("S2", amt)
        i += 1
        if not ok:
            break
        allowed += amt
        if i > 10:
            break
    assert allowed <= cap
