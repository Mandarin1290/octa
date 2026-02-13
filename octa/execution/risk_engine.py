from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


_ML_SCALING = {0: 1.0, 1: 1.25, 2: 1.5, 3: 2.0}


@dataclass(frozen=True)
class RiskDecision:
    allow: bool
    base_size: float
    final_size: float
    multiplier_applied: float
    reason: str
    risk_snapshot: Dict[str, Any]


@dataclass(frozen=True)
class RiskEngineConfig:
    ml_base_risk_pct: float = 0.01
    ml_max_gross_exposure_pct: float = 0.5
    carry_risk_budget_pct: float = 0.1
    max_carry_gross_exposure_pct: float = 0.2
    max_carry_net_exposure_pct: float = 0.1
    max_per_pair_exposure_pct: float = 0.05
    max_leverage_for_carry: float = 1.5
    carry_funding_safety_margin: float = 0.1
    carry_max_dd: float = 0.05


class RiskEngine:
    def __init__(self, cfg: Optional[RiskEngineConfig] = None) -> None:
        self.cfg = cfg or RiskEngineConfig()

    def decide_ml(
        self,
        *,
        nav: float,
        scaling_level: int,
        current_gross_exposure_pct: float,
    ) -> RiskDecision:
        base_size = max(0.0, float(nav) * float(self.cfg.ml_base_risk_pct))
        mult = _ML_SCALING.get(int(scaling_level), 1.0)
        requested = base_size * mult
        gross_cap = max(0.0, float(nav) * float(self.cfg.ml_max_gross_exposure_pct))
        remaining = max(0.0, gross_cap - (float(nav) * float(current_gross_exposure_pct)))
        final_size = min(requested, remaining)
        allow = final_size > 0.0 and base_size > 0.0
        reason = "ok" if allow else "ml_exposure_cap_or_zero_size"
        return RiskDecision(
            allow=allow,
            base_size=base_size,
            final_size=final_size,
            multiplier_applied=mult if allow else 1.0,
            reason=reason,
            risk_snapshot={
                "strategy": "ml",
                "nav": nav,
                "scaling_level": int(scaling_level),
                "requested": requested,
                "remaining_capacity": remaining,
                "gross_cap": gross_cap,
                "current_gross_exposure_pct": float(current_gross_exposure_pct),
            },
        )

    def decide_carry(
        self,
        *,
        nav: float,
        carry_confidence: float,
        expected_net_carry_after_costs: float,
        funding_cost: float,
        carry_drawdown: float,
        current_carry_gross_exposure_pct: float,
        current_carry_net_exposure_pct: float,
        current_pair_exposure_pct: float,
        leverage: float,
        live_mode: bool,
        pnl_available: bool,
    ) -> RiskDecision:
        base_size = max(0.0, float(nav) * float(self.cfg.carry_risk_budget_pct))
        conf = min(1.0, max(0.0, float(carry_confidence)))
        requested = base_size * conf

        if live_mode and (not pnl_available):
            return self._deny_carry(base_size, "pnl_unavailable", requested, nav, leverage)
        if expected_net_carry_after_costs <= 0:
            return self._deny_carry(base_size, "non_positive_expected_net_carry", requested, nav, leverage)

        lhs = float(funding_cost)
        rhs = float(expected_net_carry_after_costs) * (1.0 - float(self.cfg.carry_funding_safety_margin))
        if lhs >= rhs:
            return self._deny_carry(base_size, "funding_cost_gate", requested, nav, leverage)

        if float(carry_drawdown) <= -abs(float(self.cfg.carry_max_dd)):
            return self._deny_carry(base_size, "carry_drawdown_kill_switch", requested, nav, leverage)
        if float(leverage) > float(self.cfg.max_leverage_for_carry):
            return self._deny_carry(base_size, "carry_max_leverage_exceeded", requested, nav, leverage)
        if float(current_carry_gross_exposure_pct) >= float(self.cfg.max_carry_gross_exposure_pct):
            return self._deny_carry(base_size, "carry_gross_cap_reached", requested, nav, leverage)
        if abs(float(current_carry_net_exposure_pct)) >= float(self.cfg.max_carry_net_exposure_pct):
            return self._deny_carry(base_size, "carry_net_cap_reached", requested, nav, leverage)
        if abs(float(current_pair_exposure_pct)) >= float(self.cfg.max_per_pair_exposure_pct):
            return self._deny_carry(base_size, "carry_pair_cap_reached", requested, nav, leverage)

        remaining = max(
            0.0,
            float(nav)
            * max(
                0.0,
                float(self.cfg.max_carry_gross_exposure_pct) - float(current_carry_gross_exposure_pct),
            ),
        )
        final_size = min(requested, remaining)
        allow = final_size > 0.0
        return RiskDecision(
            allow=allow,
            base_size=base_size,
            final_size=final_size,
            multiplier_applied=conf if allow else 0.0,
            reason="ok" if allow else "carry_exposure_cap_or_zero_size",
            risk_snapshot={
                "strategy": "carry",
                "nav": nav,
                "requested": requested,
                "remaining_capacity": remaining,
                "carry_confidence": conf,
                "expected_net_carry_after_costs": expected_net_carry_after_costs,
                "funding_cost": funding_cost,
                "carry_drawdown": carry_drawdown,
                "current_carry_gross_exposure_pct": current_carry_gross_exposure_pct,
                "current_carry_net_exposure_pct": current_carry_net_exposure_pct,
                "current_pair_exposure_pct": current_pair_exposure_pct,
                "leverage": leverage,
            },
        )

    def _deny_carry(self, base_size: float, reason: str, requested: float, nav: float, leverage: float) -> RiskDecision:
        return RiskDecision(
            allow=False,
            base_size=base_size,
            final_size=0.0,
            multiplier_applied=0.0,
            reason=reason,
            risk_snapshot={
                "strategy": "carry",
                "nav": nav,
                "requested": requested,
                "leverage": leverage,
                "reason": reason,
            },
        )
