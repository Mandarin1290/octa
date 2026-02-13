from __future__ import annotations

from octa.execution.risk_engine import RiskEngine


def test_ml_scaling_and_cap() -> None:
    re = RiskEngine()
    d = re.decide_ml(nav=100000.0, scaling_level=2, current_gross_exposure_pct=0.0)
    assert d.allow is True
    assert d.multiplier_applied == 1.5
    assert d.final_size >= d.base_size

    blocked = re.decide_ml(nav=100000.0, scaling_level=3, current_gross_exposure_pct=0.5)
    assert blocked.allow is False


def test_carry_funding_gate_blocks() -> None:
    re = RiskEngine()
    d = re.decide_carry(
        nav=100000.0,
        carry_confidence=0.8,
        expected_net_carry_after_costs=0.01,
        funding_cost=0.02,
        carry_drawdown=0.0,
        current_carry_gross_exposure_pct=0.0,
        current_carry_net_exposure_pct=0.0,
        current_pair_exposure_pct=0.0,
        leverage=1.0,
        live_mode=False,
        pnl_available=True,
    )
    assert d.allow is False
    assert d.reason == "funding_cost_gate"
