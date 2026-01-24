import hashlib
import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class CostAlert:
    timestamp: str
    strategy: str
    baseline_total: float
    recent_total: float
    relative_increase: float
    severity: str
    suggested_action: Optional[str]
    evidence_hash: str = ""


class CostMonitor:
    """Monitor operational costs and detect drift/friction.

    Tracks per-strategy time series of execution costs, financing, infrastructure and slippage delta.
    Emits `CostAlert` when recent total cost rises materially versus baseline.
    """

    def __init__(
        self,
        long_window: int = 120,
        short_window: int = 30,
        drift_threshold: float = 0.2,
        min_samples: int = 40,
        max_history: int = 2000,
        max_cost_increase_for_action: float = 0.3,
        escalate_count: int = 2,
        min_alert_score: float = 0.05,
    ):
        self.long_window = int(long_window)
        self.short_window = int(short_window)
        self.drift_threshold = float(drift_threshold)
        self.min_samples = int(min_samples)
        self.max_history = int(max_history)
        self.max_cost_increase_for_action = float(max_cost_increase_for_action)
        self.escalate_count = int(escalate_count)
        self.min_alert_score = float(min_alert_score)

        from typing import Any as _Any

        self._history: Dict[str, List[Dict[str, _Any]]] = {}
        self._counts: Dict[str, int] = {}
        self.alerts: List[CostAlert] = []
        self.audit_log: List[Dict[str, Any]] = []
        self.capacities: Dict[str, Optional[float]] = {}

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _record_audit(self, action: str, details: Dict[str, Any]):
        ts = self._now_iso()
        rec = {"ts": ts, "action": action, "details": details}
        rec["evidence_hash"] = canonical_hash(rec)
        self.audit_log.append(rec)

    def set_capacity(self, strategy: str, capacity: Optional[float]):
        self.capacities[strategy] = None if capacity is None else float(capacity)
        self._record_audit("set_capacity", {"strategy": strategy, "capacity": capacity})

    def get_capacity(self, strategy: str) -> Optional[float]:
        return self.capacities.get(strategy)

    def record_costs(
        self,
        date: str,
        strategy: str,
        execution: float,
        financing: float,
        infrastructure: float,
        slippage_delta: float,
    ):
        rec = {
            "date": date,
            "execution": float(execution),
            "financing": float(financing),
            "infrastructure": float(infrastructure),
            "slippage_delta": float(slippage_delta),
        }
        self._history.setdefault(strategy, []).append(rec)
        if len(self._history[strategy]) > self.max_history:
            self._history[strategy] = self._history[strategy][-self.max_history :]
        self._record_audit("record_costs", {"strategy": strategy, **rec})

    def _select_history(self, strategy: str) -> List[Dict[str, Any]]:
        return self._history.get(strategy, [])

    def rolling_totals(self, strategy: str) -> Dict[str, Any]:
        hist = self._select_history(strategy)
        total = len(hist)
        if total == 0:
            return {"samples": 0}
        bw = min(self.long_window, total)
        rw = min(self.short_window, total)
        baseline = hist[-bw:]
        recent = hist[-rw:]

        def mean_total(rows):
            vals = [
                r["execution"]
                + r["financing"]
                + r["infrastructure"]
                + r["slippage_delta"]
                for r in rows
            ]
            return statistics.mean(vals) if vals else 0.0

        baseline_total = mean_total(baseline)
        recent_total = mean_total(recent)

        self._record_audit(
            "rolling_totals",
            {
                "strategy": strategy,
                "samples": total,
                "baseline_total": baseline_total,
                "recent_total": recent_total,
            },
        )
        return {
            "samples": total,
            "baseline_total": baseline_total,
            "recent_total": recent_total,
        }

    def evaluate(self, strategy: str) -> Optional[CostAlert]:
        stats = self.rolling_totals(strategy)
        samples = stats.get("samples", 0)
        if samples < self.min_samples:
            return None

        b = stats["baseline_total"]
        r = stats["recent_total"]
        if b <= 0:
            return None

        rel_inc = (r - b) / b
        self._record_audit(
            "evaluate_costs",
            {"strategy": strategy, "baseline": b, "recent": r, "rel_inc": rel_inc},
        )

        # simple alerting rule
        alert_score = max(0.0, min(1.0, rel_inc / self.drift_threshold))
        if alert_score < self.min_alert_score:
            return None

        if rel_inc < self.drift_threshold:
            return None

        cnt = self._counts.get(strategy, 0) + 1
        self._counts[strategy] = cnt
        severity = "warning" if cnt < self.escalate_count else "critical"

        suggested: Optional[str] = None
        current = self.get_capacity(strategy)
        if current is not None and rel_inc >= self.max_cost_increase_for_action:
            suggested = "reduce_capacity"

        ts = self._now_iso()
        alert = CostAlert(
            timestamp=ts,
            strategy=strategy,
            baseline_total=b,
            recent_total=r,
            relative_increase=rel_inc,
            severity=severity,
            suggested_action=suggested,
        )
        alert.evidence_hash = canonical_hash(
            {"ts": ts, "strategy": strategy, "rel_inc": rel_inc, "severity": severity}
        )
        self.alerts.append(alert)
        self._record_audit(
            "cost_alert",
            {
                "strategy": strategy,
                "rel_inc": rel_inc,
                "severity": severity,
                "suggested": suggested,
            },
        )
        return alert

    def get_alerts(self) -> List[CostAlert]:
        return list(self.alerts)
