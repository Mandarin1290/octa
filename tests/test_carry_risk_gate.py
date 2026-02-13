from __future__ import annotations

from octa.execution.risk_engine import RiskEngine


def test_carry_live_blocks_when_pnl_unavailable() -> None:
    re = RiskEngine()
    d = re.decide_carry(
        nav=100000.0,
        carry_confidence=0.7,
        expected_net_carry_after_costs=0.02,
        funding_cost=0.001,
        carry_drawdown=0.0,
        current_carry_gross_exposure_pct=0.0,
        current_carry_net_exposure_pct=0.0,
        current_pair_exposure_pct=0.0,
        leverage=1.0,
        live_mode=True,
        pnl_available=False,
    )
    assert d.allow is False
    assert d.reason == "pnl_unavailable"


def test_carry_allows_when_constraints_pass() -> None:
    re = RiskEngine()
    d = re.decide_carry(
        nav=100000.0,
        carry_confidence=0.7,
        expected_net_carry_after_costs=0.02,
        funding_cost=0.001,
        carry_drawdown=0.0,
        current_carry_gross_exposure_pct=0.0,
        current_carry_net_exposure_pct=0.0,
        current_pair_exposure_pct=0.0,
        leverage=1.0,
        live_mode=False,
        pnl_available=True,
    )
    assert d.allow is True
    assert d.final_size > 0.0
