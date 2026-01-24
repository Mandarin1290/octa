import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from octa_wargames.data_poisoning import (
    DataFeed,
    DetectionEngine,
    MarketExecutionGuard,
)
from octa_wargames.execution_abuse import OrderManagementSystem
from octa_wargames.strategy_sabotage import (
    StrategyContext,
    StrategyMonitor,
)


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()


@dataclass
class CircuitBreaker:
    threshold: int = 1
    failures: int = 0
    tripped: bool = False

    def record_failure(self):
        self.failures += 1
        if self.failures >= self.threshold:
            self.tripped = True

    def reset(self):
        self.failures = 0
        self.tripped = False


class FailureCascadeSimulator:
    """Orchestrates cross-system failure simulations and ensures circuit breakers/isolation."""

    def __init__(self, breaker_threshold: int = 1):
        self.detector = DetectionEngine()
        self.guard = MarketExecutionGuard(self.detector)
        self.monitor = StrategyMonitor()
        self.oms = OrderManagementSystem()
        self.circuit = CircuitBreaker(threshold=breaker_threshold)
        self.blocked_orders = 0

    def simulate_data_strategy_execution(
        self,
        primary: DataFeed,
        fallbacks: List[DataFeed],
        strategies: List[StrategyContext],
        seed: Optional[int] = None,
    ) -> Dict[str, Any]:
        # inspect primary
        corrupted, reason = self.guard.validate_feed(primary)
        if corrupted:
            # isolate strategies that depend on this feed
            res = self.monitor.assess_and_isolate(strategies)
            self.circuit.record_failure()
        else:
            res = {s.id: {"isolated": False} for s in strategies}

        # attempt to route one order per strategy; if isolated, block locally
        routed = {}
        for s in strategies:
            if not s.active:
                routed[s.id] = {"accepted": False, "reason": "isolated"}
                self.blocked_orders += 1
            else:
                r = self.route_order(s.name, "AAA", 1, "buy")
                routed[s.id] = r

        result: Dict[str, Any] = {
            "feed_corrupted": corrupted,
            "reason": reason,
            "isolation": res,
            "routed": routed,
            "breaker_tripped": self.circuit.tripped,
        }
        result["hash"] = _hash(result)
        return result

    def route_order(
        self, strategy_name: str, symbol: str, qty: float, side: str
    ) -> Dict[str, Any]:
        # find if strategy is active (simple search in oms positions) -- but caller should ensure
        # For simulation, we route directly and record blocked counts if circuit tripped
        if self.circuit.tripped:
            self.blocked_orders += 1
            return {"accepted": False, "reason": "circuit_tripped"}
        r = self.oms.receive_order(strategy_name, symbol, qty, side)
        return r

    def simulate_broker_reconciliation(
        self, mismatches: int, mismatch_threshold: int = 1
    ) -> Dict[str, Any]:
        # simulate broker failure leading to reconciliation mismatches
        if mismatches >= mismatch_threshold:
            # trip breaker and prevent NAV updates
            self.circuit.record_failure()
            nav_allowed = False
        else:
            nav_allowed = True

        result: Dict[str, Any] = {
            "mismatches": mismatches,
            "nav_allowed": nav_allowed,
            "breaker_tripped": self.circuit.tripped,
        }
        result["hash"] = _hash(result)
        return result


__all__ = ["FailureCascadeSimulator", "CircuitBreaker"]
