import pytest

from octa_strategy.lifecycle import StrategyLifecycle
from octa_strategy.shadow_gates import ShadowGateFailure, ShadowGates
from octa_strategy.state_machine import LifecycleState


def test_deviation_blocks_promotion():
    lifecycle = StrategyLifecycle("S_SHADOW")
    lifecycle.transition_to(LifecycleState.SHADOW, doc="shadow doc")
    sg = ShadowGates()
    metrics = {
        "runtime_days": 20,
        "deviation_vs_paper": 0.10,  # exceeds default 0.05
        "projected_aum": 100.0,
        "capacity_limit": 200.0,
        "incidents": 0,
        "risk_budget_utilization": 0.5,
    }
    with pytest.raises(ShadowGateFailure):
        sg.promote_if_pass(lifecycle, metrics, doc="attempt live")
    assert lifecycle.current_state == LifecycleState.SHADOW


def test_stable_shadow_allows_paper():
    lifecycle = StrategyLifecycle("S_SHADOW_OK")
    lifecycle.transition_to(LifecycleState.SHADOW, doc="shadow doc")
    sg = ShadowGates()
    metrics = {
        "runtime_days": 21,
        "deviation_vs_paper": 0.02,
        "projected_aum": 50.0,
        "capacity_limit": 100.0,
        "incidents": 0,
        "risk_budget_utilization": 0.8,
    }
    sg.promote_if_pass(lifecycle, metrics, doc="promote to paper")
    assert lifecycle.current_state == LifecycleState.PAPER
