from dataclasses import dataclass
from datetime import timedelta
from typing import Tuple

from octa_strategy.lifecycle import StrategyLifecycle
from octa_strategy.risk_budget import RiskBudget, RiskBudgetEngine


@dataclass
class AgingConfig:
    young_days: int = 90
    mature_days: int = 365
    # tightening multipliers (applied to thresholds): lower -> stricter
    young_multiplier: float = 1.0
    mature_multiplier: float = 0.9
    old_multiplier: float = 0.8


class AgingEngine:
    """Tracks strategy age since LIVE, maps to tiers and tightens thresholds.

    - `tier_for(lifecycle)` returns one of `YOUNG`, `MATURE`, `OLD`.
    - `adjust_thresholds(budget, tier)` returns a copy of the thresholds tightened per tier.
    - `check_and_escalate(...)` optionally evaluates current usage against tightened thresholds
      and emits sentinel/audit actions (best-effort; integration point).
    """

    def __init__(
        self, config: AgingConfig | None = None, audit_fn=None, sentinel_api=None
    ):
        self.config = config or AgingConfig()
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.sentinel_api = sentinel_api

    def _time_since_live(self, lifecycle: StrategyLifecycle) -> timedelta:
        # relies on lifecycle.time_in_state('LIVE') which returns timedelta since last LIVE
        return lifecycle.time_in_state("LIVE")

    def tier_for(self, lifecycle: StrategyLifecycle) -> str:
        td = self._time_since_live(lifecycle)
        days = td.total_seconds() / 86400.0
        if days < self.config.young_days:
            return "YOUNG"
        if days < self.config.mature_days:
            return "MATURE"
        return "OLD"

    def adjust_thresholds(
        self, budget: RiskBudget, tier: str
    ) -> Tuple[float, float, float]:
        if tier == "YOUNG":
            m = self.config.young_multiplier
        elif tier == "MATURE":
            m = self.config.mature_multiplier
        else:
            m = self.config.old_multiplier

        return (
            budget.warn_threshold * m,
            budget.derisk_threshold * m,
            budget.suspend_threshold * m,
        )

    def check_and_escalate(
        self,
        strategy_id: str,
        lifecycle: StrategyLifecycle,
        risk_engine: RiskBudgetEngine,
    ):
        # Fetch budget and usage
        bud = risk_engine.get_budget(strategy_id)
        usage = risk_engine.get_usage(strategy_id)
        if not bud or not usage:
            return None

        tier = self.tier_for(lifecycle)
        warn_t, derisk_t, suspend_t = self.adjust_thresholds(bud, tier)

        # compute max util as in RiskBudgetEngine
        vol_util = (
            (usage.get("vol", 0.0) / bud.vol_budget)
            if bud.vol_budget > 0
            else float("inf")
        )
        dd_util = (
            (usage.get("dd", 0.0) / bud.dd_budget)
            if bud.dd_budget > 0
            else float("inf")
        )
        exp_util = (
            (usage.get("exposure", 0.0) / bud.exposure_budget)
            if bud.exposure_budget > 0
            else float("inf")
        )
        max_util = max(vol_util, dd_util, exp_util)

        self.audit_fn(
            "aging.check",
            {"strategy_id": strategy_id, "tier": tier, "max_util": max_util},
        )

        try:
            if max_util >= warn_t and max_util < derisk_t:
                if self.sentinel_api:
                    self.sentinel_api.set_gate(
                        1, f"aging_warn:{strategy_id}:tier={tier}:util={max_util:.3f}"
                    )
            if max_util >= derisk_t and max_util <= suspend_t:
                if self.sentinel_api:
                    self.sentinel_api.set_gate(
                        2, f"aging_derisk:{strategy_id}:tier={tier}:util={max_util:.3f}"
                    )
            if max_util > suspend_t:
                if self.sentinel_api:
                    self.sentinel_api.set_gate(
                        3,
                        f"aging_suspend:{strategy_id}:tier={tier}:util={max_util:.3f}",
                    )
        except Exception:
            pass

        return {
            "tier": tier,
            "max_util": max_util,
            "thresholds": {"warn": warn_t, "derisk": derisk_t, "suspend": suspend_t},
        }
