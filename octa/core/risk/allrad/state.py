from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RiskState:
    current_drawdown: float
    max_drawdown_allowed: float
    daily_loss: float
    daily_loss_limit: float
    rolling_volatility: float
    regime_risk_score: float
    liquidity_risk_score: float
    system_health: float
    drift_score: float
    loss_streak: int = 0
    capital_exposure: float = 0.0
