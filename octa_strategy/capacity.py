from dataclasses import dataclass
from typing import Dict


@dataclass
class CapacityParams:
    adv: float
    turnover: float
    impact: float
    adv_fraction: float = 0.01
    base_scaler: float = 1.0


class CapacityEngine:
    """Per-strategy capacity estimator and allocator guard.

    - Capacity is strategy-specific and computed from ADV, turnover and impact.
    - Exceeding capacity blocks further allocations and reduces expected returns (handled externally).
    - Integrates with `allocator_api.allocate(strategy_id, amount)` and `sentinel_api.set_gate(level, reason)`.
    """

    def __init__(self, audit_fn=None, sentinel_api=None, allocator_api=None):
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.sentinel_api = sentinel_api
        self.allocator_api = allocator_api

        self._params: Dict[str, CapacityParams] = {}
        self._aum: Dict[str, float] = {}

    def register_strategy(
        self,
        strategy_id: str,
        adv: float,
        turnover: float,
        impact: float,
        adv_fraction: float = 0.01,
        base_scaler: float = 1.0,
    ) -> None:
        if strategy_id in self._params:
            raise ValueError("strategy already registered")
        self._params[strategy_id] = CapacityParams(
            adv=adv,
            turnover=turnover,
            impact=impact,
            adv_fraction=adv_fraction,
            base_scaler=base_scaler,
        )
        self._aum[strategy_id] = 0.0
        self.audit_fn(
            "capacity.register",
            {"strategy_id": strategy_id, "params": self._params[strategy_id].__dict__},
        )

    def estimate_capacity(self, strategy_id: str) -> float:
        p = self._params.get(strategy_id)
        if not p:
            return 0.0
        if p.impact <= 0 or p.turnover <= 0:
            return 0.0
        # simplified capacity formula:
        # capacity = adv * adv_fraction * (1/impact) * (1/turnover) * base_scaler
        cap = (
            p.adv
            * p.adv_fraction
            * (1.0 / p.impact)
            * (1.0 / p.turnover)
            * p.base_scaler
        )
        return cap

    def capacity_utilization(self, strategy_id: str) -> float:
        cap = self.estimate_capacity(strategy_id)
        if cap <= 0:
            return float("inf")
        return self._aum.get(strategy_id, 0.0) / cap

    def can_allocate(self, strategy_id: str, amount: float) -> bool:
        cap = self.estimate_capacity(strategy_id)
        if cap <= 0:
            return False
        return (self._aum.get(strategy_id, 0.0) + amount) <= cap

    def allocate(self, strategy_id: str, amount: float) -> bool:
        if strategy_id not in self._params:
            raise ValueError("strategy not registered")
        if not self.can_allocate(strategy_id, amount):
            # block allocation and signal sentinel
            try:
                if self.sentinel_api is not None:
                    self.sentinel_api.set_gate(2, f"capacity_exceeded:{strategy_id}")
            except Exception:
                pass
            self.audit_fn(
                "capacity.block",
                {
                    "strategy_id": strategy_id,
                    "attempt": amount,
                    "aum": self._aum.get(strategy_id, 0.0),
                    "capacity": self.estimate_capacity(strategy_id),
                },
            )
            return False

        # perform allocation via allocator
        try:
            if self.allocator_api is not None:
                self.allocator_api.allocate(strategy_id, amount)
        except Exception:
            pass
        self._aum[strategy_id] = self._aum.get(strategy_id, 0.0) + amount
        self.audit_fn(
            "capacity.allocate",
            {
                "strategy_id": strategy_id,
                "amount": amount,
                "new_aum": self._aum[strategy_id],
            },
        )
        return True

    def get_aum(self, strategy_id: str) -> float:
        return self._aum.get(strategy_id, 0.0)
