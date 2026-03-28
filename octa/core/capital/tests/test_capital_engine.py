from __future__ import annotations

from octa.core.capital.engine import CapitalEngine
from octa.core.capital.state import CapitalState
from octa.core.risk.allrad.engine import RiskDecision


def _capital_state() -> CapitalState:
    return CapitalState(
        total_equity=100_000.0,
        free_equity=100_000.0,
        used_margin=0.0,
        open_positions=0,
        net_exposure=0.0,
        gross_exposure=0.0,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
    )


def test_capital_engine_allows_trade_with_limits() -> None:
    engine = CapitalEngine()
    decision = engine.allocate(
        execution_plan={"action": "ENTER", "stop_loss": 80.0},
        allrad_decision=RiskDecision(
            allow_trade=True,
            max_exposure=1.0,
            execution_override=None,
            risk_flags={},
            reason="OK",
        ),
        capital_state=_capital_state(),
        market_state={"price": 100.0, "volatility": 0.02},
    )

    assert decision.allow_trade is True
    assert decision.position_size > 0


def test_capital_engine_blocks_on_allrad_exposure() -> None:
    engine = CapitalEngine()
    decision = engine.allocate(
        execution_plan={"action": "ENTER", "stop_loss": 80.0},
        allrad_decision=RiskDecision(
            allow_trade=True,
            max_exposure=0.0,
            execution_override=None,
            risk_flags={},
            reason="RISK_OFF",
        ),
        capital_state=_capital_state(),
        market_state={"price": 100.0, "volatility": 0.02},
    )

    assert decision.allow_trade is False
    assert decision.sizing_reason == "ALLRAD_EXPOSURE"


def test_capital_engine_blocks_on_execution_plan() -> None:
    engine = CapitalEngine()
    decision = engine.allocate(
        execution_plan={"action": "HOLD"},
        allrad_decision=RiskDecision(
            allow_trade=True,
            max_exposure=1.0,
            execution_override=None,
            risk_flags={},
            reason="OK",
        ),
        capital_state=_capital_state(),
        market_state={"price": 100.0, "volatility": 0.02},
    )

    assert decision.allow_trade is False
    assert decision.sizing_reason == "NO_ENTRY"


def test_capital_engine_reduces_size_for_central_bank_window_overlay() -> None:
    engine = CapitalEngine()
    base = engine.allocate(
        execution_plan={"action": "ENTER", "stop_loss": 80.0},
        allrad_decision=RiskDecision(
            allow_trade=True,
            max_exposure=1.0,
            execution_override=None,
            risk_flags={},
            reason="OK",
        ),
        capital_state=_capital_state(),
        market_state={"price": 100.0, "volatility": 0.02},
    )
    adjusted = engine.allocate(
        execution_plan={
            "action": "ENTER",
            "stop_loss": 80.0,
            "position_size_multiplier": 0.5,
            "risk_multiplier": 1.25,
        },
        allrad_decision=RiskDecision(
            allow_trade=True,
            max_exposure=1.0,
            execution_override=None,
            risk_flags={},
            reason="OK",
        ),
        capital_state=_capital_state(),
        market_state={"price": 100.0, "volatility": 0.02},
    )

    assert adjusted.allow_trade is True
    assert adjusted.position_size == base.position_size * 0.5
    assert adjusted.risk_flags["risk_multiplier"] == 1.25
