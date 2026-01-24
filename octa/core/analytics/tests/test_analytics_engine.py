from __future__ import annotations

from octa.core.analytics.attribution import compute_attribution
from octa.core.analytics.diagnostics import compute_diagnostics
from octa.core.analytics.performance import compute_performance
from octa.core.analytics.risk_metrics import compute_risk_metrics


def test_performance_metrics() -> None:
    equity = [100.0, 101.0, 102.0, 103.0, 104.0]
    summary = compute_performance(equity, periods_per_year=252)

    assert round(summary.cumulative_return, 6) == 0.04
    assert summary.max_drawdown <= 0.0
    assert summary.sharpe > 0


def test_risk_metrics() -> None:
    returns = [0.01, -0.02, 0.015, -0.01]
    drawdowns = [0.0, -0.02, -0.01, -0.03]
    summary = compute_risk_metrics(returns, drawdowns)

    assert summary.var <= 0
    assert summary.cvar <= 0
    assert summary.drawdown_duration >= 1


def test_attribution_summary() -> None:
    summary = compute_attribution(
        symbol_pnl={"AAA": 10.0},
        gate_pnl={"signal": 5.0},
        regime_pnl={"RISK_ON": 10.0},
        session_pnl={"us": 10.0},
    )
    assert summary.per_symbol["AAA"] == 10.0


def test_diagnostics_summary() -> None:
    summary = compute_diagnostics(
        signal_hits=[True, False, True],
        slippages=[0.01, 0.02],
        risk_vetoes=[False, True],
        capital_blocks=[False, False],
        rejection_reasons=["ALLRAD", "CAPITAL"],
    )
    assert summary.signal_accuracy == 2 / 3
    assert summary.rejection_reasons["ALLRAD"] == 1
