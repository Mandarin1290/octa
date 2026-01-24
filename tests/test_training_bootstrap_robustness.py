import numpy as np

from octa_training.core.gates import GateSpec
from octa_training.core.robustness import block_bootstrap_robustness


def test_bootstrap_disabled_by_default():
    gate = GateSpec()
    r = np.random.default_rng(0).normal(0.0, 0.01, size=300)
    out = block_bootstrap_robustness(r, gate)
    assert out.get("enabled") is False
    assert out.get("passed") is True


def test_bootstrap_fail_closed_on_missing_returns_when_enabled():
    gate = GateSpec(bootstrap_sharpe_p05_min=0.0)
    out = block_bootstrap_robustness(None, gate)
    assert out.get("enabled") is True
    assert out.get("passed") is False


def test_bootstrap_is_deterministic_given_seed():
    gate = GateSpec(
        bootstrap_sharpe_floor=0.0,
        bootstrap_sharpe_p05_min=-10.0,
        bootstrap_maxdd_p95_max=1.0,
        bootstrap_prob_sharpe_below_max=1.0,
        bootstrap_n=200,
        bootstrap_block=10,
        bootstrap_seed=7,
    )
    returns = np.full(260, 0.001, dtype=float)
    a = block_bootstrap_robustness(returns, gate)
    b = block_bootstrap_robustness(returns, gate)
    assert a["enabled"] is True and b["enabled"] is True
    assert a["passed"] == b["passed"]
    assert a["checks"]["sharpe_p05"]["value"] == b["checks"]["sharpe_p05"]["value"]
    assert a["checks"]["maxdd_p95"]["value"] == b["checks"]["maxdd_p95"]["value"]
    assert a["checks"]["prob_sharpe_below_floor"]["value"] == b["checks"]["prob_sharpe_below_floor"]["value"]
