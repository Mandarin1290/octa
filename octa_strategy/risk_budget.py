from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class RiskBudget:
    vol_budget: float
    dd_budget: float
    exposure_budget: float
    # thresholds for escalation (fractions)
    warn_threshold: float = 0.8
    derisk_threshold: float = 1.0
    suspend_threshold: float = 1.2


class RiskBudgetEngine:
    """Per-strategy risk budget manager with escalation ladder.

    - Maintains per-strategy budgets and real-time consumption.
    - Escalation ladder: warn -> derisk -> suspend.
    - Integration hooks: `sentinel_api.set_gate(level, reason)`, `allocator_api.derisk(strategy_id, factor)`.
    """

    def __init__(
        self,
        audit_fn=None,
        sentinel_api=None,
        allocator_api=None,
        suspend_repeat: int = 3,
    ):
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.sentinel_api = sentinel_api
        self.allocator_api = allocator_api
        self.suspend_repeat = suspend_repeat

        self._budgets: Dict[str, RiskBudget] = {}
        self._usage: Dict[str, Dict] = {}
        self._suspend_counts: Dict[str, int] = {}

    def register_strategy(self, strategy_id: str, budget: RiskBudget) -> None:
        if strategy_id in self._budgets:
            raise ValueError("strategy already registered")
        self._budgets[strategy_id] = budget
        self._usage[strategy_id] = {"vol": 0.0, "dd": 0.0, "exposure": 0.0}
        self._suspend_counts[strategy_id] = 0
        self.audit_fn(
            "risk.register", {"strategy_id": strategy_id, "budget": budget.__dict__}
        )

    def record_usage(
        self, strategy_id: str, vol: float, dd: float, exposure: float
    ) -> None:
        if strategy_id not in self._budgets:
            raise ValueError("strategy not registered")
        u = self._usage[strategy_id]
        u["vol"] = vol
        u["dd"] = dd
        u["exposure"] = exposure
        self.audit_fn("risk.usage", {"strategy_id": strategy_id, "usage": u})
        self._evaluate(strategy_id)

    def _evaluate(self, strategy_id: str) -> None:
        bud = self._budgets[strategy_id]
        u = self._usage[strategy_id]

        vol_util = (u["vol"] / bud.vol_budget) if bud.vol_budget > 0 else float("inf")
        dd_util = (u["dd"] / bud.dd_budget) if bud.dd_budget > 0 else float("inf")
        exp_util = (
            (u["exposure"] / bud.exposure_budget)
            if bud.exposure_budget > 0
            else float("inf")
        )

        max_util = max(vol_util, dd_util, exp_util)

        # warn
        if max_util >= bud.warn_threshold and max_util < bud.derisk_threshold:
            self.audit_fn("risk.warn", {"strategy_id": strategy_id, "util": max_util})
            try:
                if self.sentinel_api is not None:
                    self.sentinel_api.set_gate(
                        1, f"risk_warn:{strategy_id}:util={max_util:.3f}"
                    )
            except Exception:
                pass

        # derisk
        if max_util >= bud.derisk_threshold and max_util <= bud.suspend_threshold:
            self.audit_fn("risk.derisk", {"strategy_id": strategy_id, "util": max_util})
            try:
                if self.allocator_api is not None:
                    # instruct allocator to reduce risk for this strategy by a factor proportional to util
                    factor = min(0.9, 1.0 / max_util) if max_util > 0 else 0.9
                    self.allocator_api.derisk(strategy_id, factor)
            except Exception:
                pass
            try:
                if self.sentinel_api is not None:
                    self.sentinel_api.set_gate(
                        2, f"risk_derisk:{strategy_id}:util={max_util:.3f}"
                    )
            except Exception:
                pass

        # suspend (strictly above suspend threshold)
        if max_util > bud.suspend_threshold:
            self._suspend_counts[strategy_id] += 1
            self.audit_fn(
                "risk.suspend_count",
                {
                    "strategy_id": strategy_id,
                    "count": self._suspend_counts[strategy_id],
                },
            )
            if self._suspend_counts[strategy_id] >= self.suspend_repeat:
                self.audit_fn(
                    "risk.suspend", {"strategy_id": strategy_id, "util": max_util}
                )
                try:
                    if self.sentinel_api is not None:
                        self.sentinel_api.set_gate(
                            3, f"risk_suspend:{strategy_id}:util={max_util:.3f}"
                        )
                except Exception:
                    pass
                # final action: request allocator to suspend strategy
                try:
                    if self.allocator_api is not None:
                        self.allocator_api.suspend(strategy_id)
                except Exception:
                    pass

    def get_usage(self, strategy_id: str) -> Dict:
        return dict(self._usage.get(strategy_id, {}))

    def get_budget(self, strategy_id: str) -> Optional[RiskBudget]:
        return self._budgets.get(strategy_id)
