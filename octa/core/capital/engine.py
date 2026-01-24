from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from octa.core.risk.allrad.engine import RiskDecision

from .exposure import ExposureDecision, ExposureLimits, check_exposure
from .sizing import FixedFractionalSizing, MaxLossSizing, SizingResult, VolatilityAdjustedSizing
from .state import CapitalState


@dataclass(frozen=True)
class CapitalDecision:
    allow_trade: bool
    position_size: float
    capital_used: float
    exposure_after: float
    sizing_reason: str
    risk_flags: dict[str, Any]
    symbol: str | None = None
    execution_plan: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class CapitalEngineConfig:
    sizing_mode: str = "fixed_fractional"
    max_risk_pct: float = 0.01
    volatility_risk_pct: float = 0.01
    max_loss_pct: float = 0.01


class CapitalEngine:
    def __init__(
        self,
        config: CapitalEngineConfig | None = None,
        exposure_limits: ExposureLimits | None = None,
    ) -> None:
        self._config = config or CapitalEngineConfig()
        self._limits = exposure_limits or ExposureLimits()

    def allocate(
        self,
        execution_plan: Mapping[str, Any],
        allrad_decision: RiskDecision,
        capital_state: CapitalState,
        market_state: Mapping[str, Any],
    ) -> CapitalDecision:
        if not allrad_decision.allow_trade:
            return _reject("ALLRAD_BLOCK", capital_state.net_exposure)

        if execution_plan.get("action") != "ENTER":
            return _reject("NO_ENTRY", capital_state.net_exposure)

        entry_price = float(market_state.get("price", 0.0))
        stop_loss = execution_plan.get("stop_loss")
        if entry_price <= 0 or stop_loss is None:
            return _reject("INVALID_PRICE", capital_state.net_exposure)

        sizing = _sizing_strategy(self._config)
        sizing_result = sizing(entry_price, stop_loss, capital_state.total_equity, market_state)
        if sizing_result.position_size <= 0:
            return _reject("ZERO_SIZE", capital_state.net_exposure)

        position_value = sizing_result.position_size * entry_price
        exposure_decision = check_exposure(
            capital_state.total_equity,
            capital_state.net_exposure,
            capital_state.gross_exposure,
            position_value,
            self._limits,
        )
        if not exposure_decision.allow:
            return _reject(exposure_decision.reason, exposure_decision.exposure_after)

        max_exposure_value = capital_state.total_equity * allrad_decision.max_exposure
        if position_value > max_exposure_value:
            return _reject("ALLRAD_EXPOSURE", exposure_decision.exposure_after)

        risk_flags = {
            "allrad_reason": allrad_decision.reason,
            "max_exposure": allrad_decision.max_exposure,
            "exposure_after": exposure_decision.exposure_after,
        }

        return CapitalDecision(
            allow_trade=True,
            position_size=sizing_result.position_size,
            capital_used=sizing_result.capital_required,
            exposure_after=exposure_decision.exposure_after,
            sizing_reason=self._config.sizing_mode,
            risk_flags=risk_flags,
            symbol=str(market_state.get("symbol", "")) or None,
            execution_plan=dict(execution_plan),
        )


def _reject(reason: str, exposure_after: float) -> CapitalDecision:
    return CapitalDecision(
        allow_trade=False,
        position_size=0.0,
        capital_used=0.0,
        exposure_after=exposure_after,
        sizing_reason=reason,
        risk_flags={"reason": reason},
    )


def _sizing_strategy(config: CapitalEngineConfig):
    if config.sizing_mode == "volatility_adjusted":
        engine = VolatilityAdjustedSizing(base_risk_pct=config.volatility_risk_pct)
        return lambda entry, stop, equity, market: engine.size(
            entry, stop, equity, float(market.get("volatility", 0.0))
        )

    if config.sizing_mode == "max_loss":
        engine = MaxLossSizing(max_risk_pct=config.max_loss_pct)
        return lambda entry, stop, equity, _: engine.size(entry, stop, equity)

    engine = FixedFractionalSizing(risk_pct=config.max_risk_pct)
    return lambda entry, stop, equity, _: engine.size(entry, stop, equity)
