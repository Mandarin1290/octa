from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

from .policies import (
    ALLRADPolicyConfig,
    policy_availability,
    policy_drift,
    policy_liquidity,
    policy_loss,
    policy_regime,
    policy_volatility,
)
from .state import RiskState

if TYPE_CHECKING:
    from octa.core.cascade.context import CascadeContext


@dataclass(frozen=True)
class RiskDecision:
    allow_trade: bool
    max_exposure: float
    execution_override: dict[str, Any] | None
    risk_flags: dict[str, Any]
    reason: str


class ALLRADEngine:
    def __init__(self, config: ALLRADPolicyConfig | None = None) -> None:
        self._config = config or ALLRADPolicyConfig()

    def evaluate(
        self,
        cascade_context: "CascadeContext",
        portfolio_state: Mapping[str, Any],
        market_state: Mapping[str, Any],
    ) -> RiskDecision:
        state = _state_from_inputs(portfolio_state, market_state)
        regime_label = _regime_from_context(cascade_context)
        availability_ok = market_state.get("availability_ok", True)

        loss_ok, loss_reason = policy_loss(state, self._config)
        drift_ok, drift_reason = policy_drift(state, self._config)
        avail_ok, avail_reason = policy_availability(bool(availability_ok))

        exposure_cap = policy_regime(regime_label)
        liquidity_policy = policy_liquidity(state, self._config)
        vol_policy = policy_volatility(state, self._config)

        allow_trade = loss_ok and drift_ok and avail_ok
        reason = _first_reason([loss_reason, drift_reason, avail_reason], default="OK")

        max_exposure = exposure_cap
        if loss_reason == "REDUCE_RISK":
            max_exposure = min(max_exposure, 0.5)
        if liquidity_policy["exposure_scale"] < 1.0:
            max_exposure *= liquidity_policy["exposure_scale"]
        if vol_policy["reduce_exposure"]:
            max_exposure *= vol_policy["exposure_scale"]

        execution_override = None
        if liquidity_policy["force_market"]:
            execution_override = {"order_type": "MARKET"}
        if vol_policy["reduce_exposure"]:
            execution_override = execution_override or {}
            execution_override["reduce_exposure"] = True

        risk_flags = {
            "loss_reason": loss_reason,
            "drift_reason": drift_reason,
            "availability": avail_reason,
            "regime": regime_label,
            "liquidity_risk": state.liquidity_risk_score,
            "volatility": state.rolling_volatility,
            "max_exposure": max_exposure,
        }

        return RiskDecision(
            allow_trade=allow_trade,
            max_exposure=max_exposure,
            execution_override=execution_override,
            risk_flags=risk_flags,
            reason=reason,
        )


def _state_from_inputs(
    portfolio_state: Mapping[str, Any], market_state: Mapping[str, Any]
) -> RiskState:
    return RiskState(
        current_drawdown=float(portfolio_state.get("current_drawdown", 0.0)),
        max_drawdown_allowed=float(portfolio_state.get("max_drawdown_allowed", 0.0)),
        daily_loss=float(portfolio_state.get("daily_loss", 0.0)),
        daily_loss_limit=float(portfolio_state.get("daily_loss_limit", 0.0)),
        rolling_volatility=float(market_state.get("rolling_volatility", 0.0)),
        regime_risk_score=float(market_state.get("regime_risk_score", 0.0)),
        liquidity_risk_score=float(market_state.get("liquidity_risk_score", 0.0)),
        system_health=float(market_state.get("system_health", 1.0)),
        drift_score=float(market_state.get("drift_score", 0.0)),
        loss_streak=int(portfolio_state.get("loss_streak", 0)),
        capital_exposure=float(portfolio_state.get("capital_exposure", 0.0)),
    )


def _regime_from_context(context: "CascadeContext") -> str:
    artifacts = context.artifacts.get("global_regime", {})
    if isinstance(artifacts, dict):
        regime_payload = next(iter(artifacts.values()), {})
        if isinstance(regime_payload, dict):
            return str(regime_payload.get("regime_label", "RISK_ON"))
    return "RISK_ON"


def _first_reason(reasons: list[str], default: str) -> str:
    for reason in reasons:
        if reason not in {"OK", "REDUCE_RISK"}:
            return reason
    if "REDUCE_RISK" in reasons:
        return "REDUCE_RISK"
    return default
