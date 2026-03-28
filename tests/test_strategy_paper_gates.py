import pytest

from octa_strategy.lifecycle import StrategyLifecycle
from octa_strategy.paper_gates import GateFailure, PaperGates
from octa_strategy.state_machine import LifecycleState


def test_failing_metric_blocks_promotion():
    lifecycle = StrategyLifecycle("S_PAPER")
    lifecycle.transition_to(LifecycleState.SHADOW, doc="shadow doc")
    lifecycle.transition_to(LifecycleState.PAPER, doc="paper doc")
    pg = PaperGates()
    # failing because incidents > 0
    metrics = {
        "runtime_days": 10,
        "max_drawdown": 0.05,
        "sharpe": 1.0,
        "sortino": 1.2,
        "slippage_diff": 0.01,
        "incidents": 1,
        "max_corr": 0.2,
    }
    with pytest.raises(GateFailure):
        pg.promote_if_pass(lifecycle, metrics, doc="promote attempt")
    assert lifecycle.current_state == LifecycleState.PAPER


def test_passing_metrics_allow_transition():
    lifecycle = StrategyLifecycle("S_PAPER_OK")
    lifecycle.transition_to(LifecycleState.SHADOW, doc="shadow doc")
    lifecycle.transition_to(LifecycleState.PAPER, doc="paper doc")
    pg = PaperGates()
    metrics = {
        "runtime_days": 8,
        "max_drawdown": 0.05,
        "sharpe": 0.8,
        "sortino": 1.0,
        "slippage_diff": 0.01,
        "incidents": 0,
        "max_corr": 0.3,
    }
    pg.promote_if_pass(lifecycle, metrics, doc="live promotion doc")
    assert lifecycle.current_state == LifecycleState.LIVE
