from __future__ import annotations

from octa.core.research.robustness.monte_carlo import run_monte_carlo


def test_monte_carlo_basic() -> None:
    returns = [0.01, -0.005, 0.002, 0.003, -0.001] * 5
    report = run_monte_carlo(
        returns,
        {"n_sims": 10, "block_size": 2, "dd_limit": 0.2, "stress_multipliers": [1.0]},
        seed=42,
        run_id="test",
        gate="global_1d",
        timeframe="1D",
    )
    assert report.ok
