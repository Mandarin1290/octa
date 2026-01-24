from __future__ import annotations

from octa.core.cascade.context import CascadeContext
from octa.core.risk.allrad.engine import ALLRADEngine


def test_allrad_blocks_on_drawdown() -> None:
    engine = ALLRADEngine()
    ctx = CascadeContext()

    decision = engine.evaluate(
        ctx,
        {
            "current_drawdown": 0.15,
            "max_drawdown_allowed": 0.1,
            "daily_loss": 0.0,
            "daily_loss_limit": 0.05,
        },
        {"rolling_volatility": 0.02, "liquidity_risk_score": 0.1},
    )

    assert decision.allow_trade is False
    assert decision.reason == "DRAWDOWN_LIMIT"


def test_allrad_regime_risk_off_caps_exposure() -> None:
    engine = ALLRADEngine()
    ctx = CascadeContext()
    ctx.artifacts["global_regime"] = {
        "AAA": {"regime_label": "RISK_OFF"}
    }

    decision = engine.evaluate(
        ctx,
        {
            "current_drawdown": 0.0,
            "max_drawdown_allowed": 0.2,
            "daily_loss": 0.0,
            "daily_loss_limit": 0.05,
        },
        {"rolling_volatility": 0.02, "liquidity_risk_score": 0.1},
    )

    assert decision.allow_trade is True
    assert decision.max_exposure == 0.0


def test_allrad_volatility_spike_sets_override() -> None:
    engine = ALLRADEngine()
    ctx = CascadeContext()

    decision = engine.evaluate(
        ctx,
        {
            "current_drawdown": 0.0,
            "max_drawdown_allowed": 0.2,
            "daily_loss": 0.0,
            "daily_loss_limit": 0.05,
        },
        {"rolling_volatility": 0.1, "liquidity_risk_score": 0.1},
    )

    assert decision.allow_trade is True
    assert decision.execution_override is not None
    assert decision.execution_override.get("reduce_exposure") is True
