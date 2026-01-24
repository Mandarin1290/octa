from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class ExpectationRecord:
    strategy_id: str
    expected_return: float
    expected_vol: float
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")


@dataclass
class RealizedRecord:
    strategy_id: str
    realized_return: float
    realized_vol: float
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")


class AttributionEngine:
    """Store ex-ante expectations, record ex-post results, compute deviations and trigger reviews.

    - Expectations are versioned per strategy.
    - Realized events are appended; deviations computed against latest expectation.
    - If deviation (absolute) exceeds threshold, trigger review via `sentinel_api` and audit via `audit_fn`.
    """

    def __init__(
        self, audit_fn=None, sentinel_api=None, deviation_threshold: float = 0.03
    ):
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.sentinel_api = sentinel_api
        self.deviation_threshold = deviation_threshold

        self._expectations: Dict[str, List[ExpectationRecord]] = {}
        self._realized: Dict[str, List[RealizedRecord]] = {}

    def record_expectation(
        self, strategy_id: str, expected_return: float, expected_vol: float
    ) -> None:
        rec = ExpectationRecord(
            strategy_id=strategy_id,
            expected_return=float(expected_return),
            expected_vol=float(expected_vol),
        )
        self._expectations.setdefault(strategy_id, []).append(rec)
        self.audit_fn(
            "attribution.expectation",
            {
                "strategy_id": strategy_id,
                "expected_return": rec.expected_return,
                "expected_vol": rec.expected_vol,
                "timestamp": rec.timestamp,
            },
        )

    def latest_expectation(self, strategy_id: str) -> Optional[ExpectationRecord]:
        lst = self._expectations.get(strategy_id, [])
        return lst[-1] if lst else None

    def record_realized(
        self, strategy_id: str, realized_return: float, realized_vol: float
    ) -> None:
        rec = RealizedRecord(
            strategy_id=strategy_id,
            realized_return=float(realized_return),
            realized_vol=float(realized_vol),
        )
        self._realized.setdefault(strategy_id, []).append(rec)
        self.audit_fn(
            "attribution.realized",
            {
                "strategy_id": strategy_id,
                "realized_return": rec.realized_return,
                "realized_vol": rec.realized_vol,
                "timestamp": rec.timestamp,
            },
        )
        # evaluate deviation against latest expectation
        exp = self.latest_expectation(strategy_id)
        if exp is not None:
            dev = abs(rec.realized_return - exp.expected_return)
            if dev >= self.deviation_threshold:
                # trigger review
                try:
                    if self.sentinel_api is not None:
                        self.sentinel_api.set_gate(
                            2, f"attribution_deviation:{strategy_id}:dev={dev:.4f}"
                        )
                except Exception:
                    pass
                self.audit_fn(
                    "attribution.deviation",
                    {
                        "strategy_id": strategy_id,
                        "deviation": dev,
                        "threshold": self.deviation_threshold,
                    },
                )

    def total_realized(self, strategy_id: str) -> float:
        return sum(r.realized_return for r in self._realized.get(strategy_id, []))

    def total_expected(self, strategy_id: str) -> float:
        exp = self.latest_expectation(strategy_id)
        return exp.expected_return if exp else 0.0

    def deviation_metrics(self, strategy_id: str) -> Dict:
        exp = self.latest_expectation(strategy_id)
        realized_total = self.total_realized(strategy_id)
        if not exp:
            return {"reconciles": False, "deviation": None}
        dev = realized_total - exp.expected_return
        return {
            "expected": exp.expected_return,
            "realized_total": realized_total,
            "deviation": dev,
            "reconciles": abs(dev) < 1e-9,
        }

    def requires_review(self, strategy_id: str) -> bool:
        exp = self.latest_expectation(strategy_id)
        if not exp:
            return False
        # compare last realized event
        lst = self._realized.get(strategy_id, [])
        if not lst:
            return False
        last = lst[-1]
        dev = abs(last.realized_return - exp.expected_return)
        return dev >= self.deviation_threshold
