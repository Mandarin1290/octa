from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .state import RiskState


@dataclass(frozen=True)
class ALLRADPolicyConfig:
    loss_streak_limit: int = 3
    liquidity_risk_high: float = 0.7
    drift_threshold: float = 0.8
    volatility_spike: float = 0.05


def policy_loss(state: RiskState, config: ALLRADPolicyConfig) -> tuple[bool, str]:
    if state.daily_loss > state.daily_loss_limit:
        return False, "DAILY_LOSS_LIMIT"
    if abs(state.current_drawdown) > state.max_drawdown_allowed:
        return False, "DRAWDOWN_LIMIT"
    if state.loss_streak >= config.loss_streak_limit:
        return True, "REDUCE_RISK"
    return True, "OK"


def policy_regime(regime_label: str) -> float:
    if regime_label == "RISK_OFF":
        return 0.0
    if regime_label == "REDUCE":
        return 0.3
    return 1.0


def policy_liquidity(state: RiskState, config: ALLRADPolicyConfig) -> dict[str, Any]:
    if state.liquidity_risk_score >= config.liquidity_risk_high:
        return {"force_market": True, "exposure_scale": 0.5}
    return {"force_market": False, "exposure_scale": 1.0}


def policy_drift(state: RiskState, config: ALLRADPolicyConfig) -> tuple[bool, str]:
    if state.drift_score >= config.drift_threshold:
        return False, "DRIFT_LIMIT"
    return True, "OK"


def policy_availability(availability_ok: bool) -> tuple[bool, str]:
    if not availability_ok:
        return False, "DATA_UNAVAILABLE"
    return True, "OK"


def policy_volatility(state: RiskState, config: ALLRADPolicyConfig) -> dict[str, Any]:
    if state.rolling_volatility >= config.volatility_spike:
        return {"reduce_exposure": True, "exposure_scale": 0.5}
    return {"reduce_exposure": False, "exposure_scale": 1.0}
