from __future__ import annotations

from octa.core.capital.engine import CapitalDecision
from octa.core.portfolio.engine import PortfolioEngine
from octa.core.portfolio.state import PortfolioState


def _portfolio_state(drawdown: float = 0.0) -> PortfolioState:
    return PortfolioState(
        positions={},
        net_exposure=0.0,
        gross_exposure=0.0,
        sector_exposure={},
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        portfolio_drawdown=drawdown,
        rolling_volatility=0.0,
    )


def test_portfolio_blocks_on_drawdown() -> None:
    engine = PortfolioEngine()
    decisions = [
        CapitalDecision(True, 10.0, 1_000.0, 1_000.0, "fixed", {}),
    ]
    decision = engine.aggregate(decisions, _portfolio_state(drawdown=0.2), {"total_equity": 100_000.0})

    assert decision.allow_trades is False
    assert decision.reason == "DRAWDOWN_LIMIT"


def test_portfolio_blocks_on_correlation() -> None:
    engine = PortfolioEngine()
    decisions = [
        CapitalDecision(True, 10.0, 1_000.0, 1_000.0, "fixed", {}),
        CapitalDecision(True, 12.0, 1_200.0, 1_200.0, "fixed", {}),
    ]
    returns = {
        "AAA": [0.01, 0.02, 0.015, 0.01],
        "BBB": [0.011, 0.021, 0.016, 0.012],
    }
    decision = engine.aggregate(decisions, _portfolio_state(), {"returns": returns, "total_equity": 100_000.0})

    assert decision.allow_trades is False
    assert decision.reason in {"OK", "CONCENTRATION_LIMIT", "DRAWDOWN_LIMIT"}


def test_portfolio_allows_under_limits() -> None:
    engine = PortfolioEngine()
    decisions = [
        CapitalDecision(True, 5.0, 500.0, 500.0, "fixed", {}),
    ]
    decision = engine.aggregate(decisions, _portfolio_state(), {"returns": {}, "total_equity": 100_000.0})

    assert decision.allow_trades is True
