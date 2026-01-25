from __future__ import annotations

from octa.core.execution.costs import CostConfig, apply_costs, estimate_costs


def test_estimate_costs() -> None:
    trades = [{"size_frac": 0.1, "price": 100.0}, {"size_frac": 0.2, "price": 101.0}]
    costs = estimate_costs(trades, {"volatility": 0.02, "liquidity": 1.0}, CostConfig())
    assert costs.total_cost_bps > 0


def test_apply_costs() -> None:
    pnl = [0.01, -0.005, 0.002]
    costs = estimate_costs([{"size_frac": 0.1, "price": 100.0}], {}, CostConfig())
    net = apply_costs(pnl, costs)
    assert len(net) == len(pnl)
