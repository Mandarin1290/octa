from typing import Any, List

from octa_strategy.auto_derisk import AutoDerisk, AutoDeriskConfig
from octa_strategy.risk_budget import RiskBudget, RiskBudgetEngine


class FakeAllocator:
    def __init__(self, risk_engine: RiskBudgetEngine):
        self.calls: List[Any] = []
        self.risk_engine = risk_engine

    def derisk(self, strategy_id: str, factor: float):
        self.calls.append(("derisk", strategy_id, factor))
        # simulate exposure reduction by applying factor to recorded usage exposure
        u = self.risk_engine._usage.get(strategy_id)
        if u is not None:
            u["exposure"] = u.get("exposure", 0.0) * factor

    def suspend(self, strategy_id: str):
        self.calls.append(("suspend", strategy_id))


class FakeSentinel:
    def __init__(self):
        self.gates = []

    def set_gate(self, level: int, reason: str):
        self.gates.append((level, reason))


def test_derisk_reduces_exposure():
    rengine = RiskBudgetEngine(audit_fn=lambda e, p: None)
    budget = RiskBudget(vol_budget=1.0, dd_budget=100.0, exposure_budget=1000.0)
    rengine.register_strategy("S1", budget)
    # initial exposure high
    rengine.record_usage("S1", vol=0.1, dd=1.0, exposure=500.0)

    allocator = FakeAllocator(rengine)
    sentinel = FakeSentinel()
    ad = AutoDerisk(
        audit_fn=lambda e, p: None,
        sentinel_api=sentinel,
        allocator_api=allocator,
        config=AutoDeriskConfig(
            min_factor=0.6,
            cooldown_seconds=0,
            max_attempts=3,
            effectiveness_threshold=0.001,
        ),
    )
    ad.register_strategy("S1", scale=1.0)

    # healthy low health score causes strong derisk
    before = rengine.get_usage("S1")["exposure"]
    ad.process("S1", health_score=0.2, current_exposure=before)
    after = rengine.get_usage("S1")["exposure"]
    assert (
        "derisk",
        "S1",
    ) == (allocator.calls[0][0], allocator.calls[0][1])
    assert after < before


def test_ineffective_derisk_escalates():
    rengine = RiskBudgetEngine(audit_fn=lambda e, p: None)
    budget = RiskBudget(vol_budget=1.0, dd_budget=100.0, exposure_budget=1000.0)
    rengine.register_strategy("S2", budget)
    rengine.record_usage("S2", vol=0.1, dd=1.0, exposure=400.0)

    # allocator that does NOT reduce exposure
    class NoopAllocator:
        def __init__(self):
            self.calls: List[Any] = []

        def derisk(self, strategy_id: str, factor: float):
            self.calls.append(("derisk", strategy_id, factor))

        def suspend(self, strategy_id: str):
            self.calls.append(("suspend", strategy_id))

    allocator = NoopAllocator()
    sentinel = FakeSentinel()
    # short cooldown so we can call repeatedly
    ad = AutoDerisk(
        audit_fn=lambda e, p: None,
        sentinel_api=sentinel,
        allocator_api=allocator,
        config=AutoDeriskConfig(
            min_factor=0.6,
            cooldown_seconds=0,
            max_attempts=2,
            effectiveness_threshold=0.05,
        ),
    )
    ad.register_strategy("S2", scale=1.0)

    # call process twice; allocator does nothing so attempts should escalate to suspend
    u = rengine.get_usage("S2")["exposure"]
    ad.process("S2", health_score=0.1, current_exposure=u)
    ad.process("S2", health_score=0.1, current_exposure=u)

    # after max_attempts (2) escalate -> suspend called and sentinel gate set
    assert any(c[0] == "suspend" for c in allocator.calls)
    assert any(g[0] == 3 for g in sentinel.gates)
