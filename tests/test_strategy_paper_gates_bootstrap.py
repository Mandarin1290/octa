import pytest

from octa_strategy.lifecycle import StrategyLifecycle
from octa_strategy.paper_gates import GateFailure, PaperGates
from octa_strategy.state_machine import LifecycleState


def test_paper_gate_bootstrap_requires_returns_when_configured():
    lifecycle = StrategyLifecycle("S_PAPER_BOOT_REQ")
    lifecycle.transition_to(LifecycleState.PAPER, doc="paper doc")
    pg = PaperGates(
        thresholds={
            "bootstrap_sharpe_p05_min": 0.2,
            "bootstrap_maxdd_p95_max": 0.25,
            "bootstrap_prob_sharpe_below_max": 0.8,
        }
    )
    metrics = {
        "runtime_days": 10,
        "max_drawdown": 0.05,
        "sharpe": 1.0,
        "sortino": 1.2,
        "slippage_diff": 0.01,
        "incidents": 0,
        "max_corr": 0.2,
        # returns missing on purpose
    }
    with pytest.raises(GateFailure):
        pg.promote_if_pass(lifecycle, metrics, doc="promote attempt")


def test_paper_gate_bootstrap_can_pass_with_returns():
    lifecycle = StrategyLifecycle("S_PAPER_BOOT_OK")
    lifecycle.transition_to(LifecycleState.PAPER, doc="paper doc")

    # simple synthetic daily returns: small positive drift, low noise
    returns = [0.001] * 260

    pg = PaperGates(
        thresholds={
            "bootstrap_sharpe_floor": 0.0,
            "bootstrap_n": 200,
            "bootstrap_block": 10,
            "bootstrap_seed": 7,
            "bootstrap_sharpe_p05_min": 0.0,
            "bootstrap_maxdd_p95_max": 0.10,
            "bootstrap_prob_sharpe_below_max": 0.01,
        }
    )

    metrics = {
        "runtime_days": 8,
        "max_drawdown": 0.05,
        "sharpe": 0.8,
        "sortino": 1.0,
        "slippage_diff": 0.01,
        "incidents": 0,
        "max_corr": 0.3,
        "returns": returns,
    }

    pg.promote_if_pass(lifecycle, metrics, doc="shadow promotion doc")
    assert lifecycle.current_state == LifecycleState.SHADOW
