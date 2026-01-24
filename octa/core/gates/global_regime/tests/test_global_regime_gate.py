from __future__ import annotations

from octa.core.cascade.contracts import GateDecision
from octa.core.gates.global_regime.gate import GlobalRegimeGate, GlobalRegimeGateConfig, RegimeLabel


def _series(start: float, daily_returns: list[float]) -> list[float]:
    prices = [start]
    for ret in daily_returns:
        prices.append(prices[-1] * (1.0 + ret))
    return prices


def test_regime_risk_on_for_uptrend() -> None:
    returns = [0.002] * 160
    prices = _series(100.0, returns)

    gate = GlobalRegimeGate(price_series=prices)
    outcome = gate.evaluate(["SPY"])

    assert outcome.decision == GateDecision.PASS
    assert outcome.artifacts["regime_label"] == RegimeLabel.RISK_ON.value
    metrics = outcome.artifacts["metrics"]
    assert "drawdown" in metrics
    assert "volatility" in metrics
    assert outcome.artifacts["window"]["points"] == len(prices)


def test_regime_halt_for_crash_drawdown() -> None:
    returns = [0.0] * 50 + [-0.02] * 80
    prices = _series(100.0, returns)

    config = GlobalRegimeGateConfig(halt_drawdown=0.2)
    gate = GlobalRegimeGate(config=config, price_series=prices)
    outcome = gate.evaluate(["SPY"])

    assert outcome.decision == GateDecision.FAIL
    assert outcome.artifacts["regime_label"] == RegimeLabel.HALT.value


def test_regime_reduce_or_risk_off_for_choppy_vol() -> None:
    returns = [0.05, -0.05] * 80
    prices = _series(100.0, returns)

    config = GlobalRegimeGateConfig(risk_off_vol=0.03, reduce_vol=0.02, risk_off_trend=0.0)
    gate = GlobalRegimeGate(config=config, price_series=prices)
    outcome = gate.evaluate(["SPY"])

    assert outcome.artifacts["regime_label"] in {
        RegimeLabel.RISK_OFF.value,
        RegimeLabel.REDUCE.value,
    }
