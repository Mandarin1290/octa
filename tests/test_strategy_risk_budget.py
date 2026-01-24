from octa_strategy.risk_budget import RiskBudget, RiskBudgetEngine


def test_breach_triggers_derisk_and_suspend():
    audit = []

    class SentinelMock:
        def __init__(self):
            self.calls = []

        def set_gate(self, level, reason):
            self.calls.append((level, reason))

    class AllocatorMock:
        def __init__(self):
            self.derisk_calls = []
            self.suspend_calls = []

        def derisk(self, strategy_id, factor):
            self.derisk_calls.append((strategy_id, factor))

        def suspend(self, strategy_id):
            self.suspend_calls.append(strategy_id)

    sentinel = SentinelMock()
    allocator = AllocatorMock()
    engine = RiskBudgetEngine(
        audit_fn=lambda e, p: audit.append((e, p)),
        sentinel_api=sentinel,
        allocator_api=allocator,
        suspend_repeat=2,
    )

    budget = RiskBudget(vol_budget=1.0, dd_budget=1.0, exposure_budget=100.0)
    engine.register_strategy("S1", budget)

    # first breach: derisk expected when util >= 1.0
    engine.record_usage("S1", vol=1.2, dd=0.2, exposure=10.0)
    assert len(allocator.derisk_calls) >= 1
    assert any(c[0] == "S1" for c in allocator.derisk_calls)

    # repeated severe breaches trigger suspension after suspend_repeat
    engine.record_usage("S1", vol=1.5, dd=0.2, exposure=10.0)
    engine.record_usage("S1", vol=1.5, dd=0.2, exposure=10.0)
    assert "S1" in allocator.suspend_calls
    assert any(c[0] == 3 for c in sentinel.calls)
