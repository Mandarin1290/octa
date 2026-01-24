from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List


class Tier(Enum):
    SEED = "SEED"
    GROWTH = "GROWTH"
    INSTITUTIONAL = "INSTITUTIONAL"


@dataclass
class TierMultipliers:
    max_leverage: float
    max_participation: float
    max_concentration: float


class CapitalTierEngine:
    """Derives capital tier from AUM and emits audited transitions.

    Attach to an `AUMState` via `attach(aum_state)` to receive snapshots and
    automatically compute tier transitions.
    """

    def __init__(
        self,
        thresholds: Dict[str, float] | None = None,
        multipliers: Dict[Tier, TierMultipliers] | None = None,
        audit_fn: Callable[[str, Dict[str, Any]], None] | None = None,
    ):
        # thresholds: seed_max, growth_max (institutional above growth_max)
        t = thresholds or {"seed_max": 1_000_000.0, "growth_max": 10_000_000.0}
        self.seed_max = float(t.get("seed_max", 1_000_000.0))
        self.growth_max = float(t.get("growth_max", 10_000_000.0))
        self.audit_fn = audit_fn or (lambda e, p: None)
        self._last_tier: Tier | None = None
        self._history: List[Dict[str, Any]] = []
        self.multipliers = multipliers or {
            Tier.SEED: TierMultipliers(
                max_leverage=1.0, max_participation=0.2, max_concentration=0.05
            ),
            Tier.GROWTH: TierMultipliers(
                max_leverage=2.0, max_participation=0.5, max_concentration=0.1
            ),
            Tier.INSTITUTIONAL: TierMultipliers(
                max_leverage=3.0, max_participation=0.8, max_concentration=0.2
            ),
        }

    def determine_tier(self, aum_total: float) -> Tier:
        aum = float(aum_total)
        if aum <= self.seed_max:
            return Tier.SEED
        if aum <= self.growth_max:
            return Tier.GROWTH
        return Tier.INSTITUTIONAL

    def multipliers_for(self, tier: Tier) -> TierMultipliers:
        return self.multipliers[tier]

    def attach(self, aum_state) -> None:
        # subscribe to snapshots
        def cb(snap):
            new_tier = self.determine_tier(snap.computed_total)
            if new_tier != self._last_tier:
                payload = {
                    "timestamp": snap.timestamp,
                    "previous_tier": self._last_tier.value if self._last_tier else None,
                    "new_tier": new_tier.value,
                    "aum_total": snap.computed_total,
                }
                self._history.append(payload)
                self.audit_fn("capital.tier.transition", payload)
                self._last_tier = new_tier

        aum_state.subscribe(cb)

    def get_current_tier(self) -> Tier | None:
        return self._last_tier

    def history(self) -> List[Dict[str, Any]]:
        return list(self._history)
