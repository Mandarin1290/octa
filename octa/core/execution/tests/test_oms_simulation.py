from __future__ import annotations

from pathlib import Path

from octa.core.capital.engine import CapitalDecision
from octa.core.execution.oms import OMS, OMSConfig
from octa.core.portfolio.engine import PortfolioDecision


def test_oms_simulation_generates_report(tmp_path: Path) -> None:
    decision = CapitalDecision(
        allow_trade=True,
        position_size=10.0,
        capital_used=1_000.0,
        exposure_after=1_000.0,
        sizing_reason="fixed",
        risk_flags={},
        symbol="AAA",
        execution_plan={
            "action": "ENTER",
            "side": "BUY",
            "order_type": "LIMIT",
            "limit_price": 100.0,
        },
    )
    portfolio = PortfolioDecision(
        allow_trades=True,
        approved_trades=[decision],
        blocked_trades=[],
        exposure_after=1_000.0,
        correlation_penalty=1.0,
        portfolio_risk_flags={},
        reason="OK",
    )

    audit_path = tmp_path / "oms.jsonl"
    oms = OMS(OMSConfig(simulation=True, audit_path=audit_path))
    reports = oms.submit(portfolio)

    assert reports
    assert reports[0].status.value == "FILLED"
    assert audit_path.exists()
    assert "\"order_id\"" in audit_path.read_text(encoding="utf-8")
