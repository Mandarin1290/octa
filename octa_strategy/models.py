from dataclasses import dataclass, field
from datetime import datetime
from typing import List


@dataclass(frozen=True)
class StrategyMeta:
    strategy_id: str
    owner: str
    asset_classes: List[str]
    risk_budget: float
    holding_period_days: int
    expected_turnover_per_month: float
    lifecycle_state: str
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")

    def as_dict(self):
        return {
            "strategy_id": self.strategy_id,
            "owner": self.owner,
            "asset_classes": list(self.asset_classes),
            "risk_budget": float(self.risk_budget),
            "holding_period_days": int(self.holding_period_days),
            "expected_turnover_per_month": float(self.expected_turnover_per_month),
            "lifecycle_state": self.lifecycle_state,
            "created_at": self.created_at,
        }
