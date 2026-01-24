import hashlib
import json
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class ErosionAlert:
    timestamp: str
    strategy: str
    slippage_baseline: Optional[float]
    slippage_recent: Optional[float]
    impact_baseline: Optional[float]
    impact_recent: Optional[float]
    fill_baseline: Optional[float]
    fill_recent: Optional[float]
    erosion_score: float
    suggested_capacity: Optional[float]
    severity: str
    evidence_hash: str = ""


class CapacityErosionMonitor:
    """Monitor capacity erosion signals and suggest capacity scaling.

    - Records per-strategy time series of `slippage`, `impact`, and `fill_ratio`.
    - Computes baseline (long window) and recent (short window) stats.
    - If slippage or impact grow or fill_ratio decays beyond thresholds,
      an erosion alert is emitted and a suggested capacity scaling is provided.

    API:
    - record_metrics(date, strategy, slippage, impact, fill_ratio)
    - evaluate(strategy) -> Optional[ErosionAlert]
    - set_capacity(strategy, capacity)
    - get_capacity(strategy)
    """

    def __init__(
        self,
        long_window: int = 120,
        short_window: int = 30,
        slippage_increase_pct: float = 0.5,
        impact_increase_pct: float = 0.5,
        fill_drop_pct: float = 0.2,
        min_samples: int = 40,
        max_history: int = 2000,
        max_reduction: float = 0.5,
        escalate_count: int = 2,
        min_erosion_score: float = 0.15,
    ):
        self.long_window = int(long_window)
        self.short_window = int(short_window)
        self.slippage_increase_pct = float(slippage_increase_pct)
        self.impact_increase_pct = float(impact_increase_pct)
        self.fill_drop_pct = float(fill_drop_pct)
        self.min_samples = int(min_samples)
        self.max_history = int(max_history)
        self.max_reduction = float(max_reduction)
        self.escalate_count = int(escalate_count)
        self.min_erosion_score = float(min_erosion_score)

        # strategy -> list of metric dicts {date, slippage, impact, fill_ratio}
        self._history: Dict[str, List[Dict[str, Any]]] = {}
        self._counts: Dict[str, int] = {}
        self.alerts: List[ErosionAlert] = []
        self.audit_log: List[Dict[str, Any]] = []
        # strategy -> current capacity (None if unknown)
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

    def record_metrics(
        self,
        date: str,
        strategy: str,
        slippage: float,
        impact: float,
        fill_ratio: float,
    ):
        rec = {
            "date": date,
            "slippage": float(slippage),
            "impact": float(impact),
            "fill_ratio": float(fill_ratio),
        }
        self._history.setdefault(strategy, []).append(rec)
        if len(self._history[strategy]) > self.max_history:
            self._history[strategy] = self._history[strategy][-self.max_history :]
        self._record_audit("record_metrics", {"strategy": strategy, **rec})

    def _select_history(self, strategy: str) -> List[Dict[str, Any]]:
        return self._history.get(strategy, [])

    def _mean(self, xs: List[float]) -> float:
        return statistics.mean(xs) if xs else 0.0

    def rolling_stats(self, strategy: str) -> Dict[str, Any]:
        hist = self._select_history(strategy)
        total = len(hist)
        if total == 0:
            return {"samples": 0}
        baseline_n = min(self.long_window, total)
        recent_n = min(self.short_window, total)
        baseline = hist[-baseline_n:]
        recent = hist[-recent_n:]

        def agg(rows, key):
            vals = [r[key] for r in rows]
            return {
                "mean": self._mean(vals),
                "std": math.sqrt(_var(vals)) if len(vals) > 0 else 0.0,
                "n": len(vals),
            }

        stats = {
            "samples": total,
            "slippage_baseline": agg(baseline, "slippage"),
            "slippage_recent": agg(recent, "slippage"),
            "impact_baseline": agg(baseline, "impact"),
            "impact_recent": agg(recent, "impact"),
            "fill_baseline": agg(baseline, "fill_ratio"),
            "fill_recent": agg(recent, "fill_ratio"),
        }
        self._record_audit(
            "rolling_stats",
            {
                "strategy": strategy,
                "stats_summary": {
                    k: (
                        v
                        if not isinstance(v, dict)
                        else {"n": v.get("n"), "mean": v.get("mean")}
                    )
                    for k, v in stats.items()
                },
            },
        )
        return stats

    def evaluate(self, strategy: str) -> Optional[ErosionAlert]:
        stats = self.rolling_stats(strategy)
        samples = stats.get("samples", 0)
        if samples < self.min_samples:
            return None

        sb = stats["slippage_baseline"]["mean"]
        sr = stats["slippage_recent"]["mean"]
        ib = stats["impact_baseline"]["mean"]
        ir = stats["impact_recent"]["mean"]
        fb = stats["fill_baseline"]["mean"]
        fr = stats["fill_recent"]["mean"]

        # compute normalized signals
        slippage_inc = 0.0 if sb <= 0 else max(0.0, (sr - sb) / sb)
        impact_inc = 0.0 if ib <= 0 else max(0.0, (ir - ib) / ib)
        fill_drop = 0.0 if fb <= 0 else max(0.0, (fb - fr) / fb)

        # score each component relative to configured thresholds
        s_slip = min(1.0, slippage_inc / self.slippage_increase_pct)
        s_imp = min(1.0, impact_inc / self.impact_increase_pct)
        s_fill = min(1.0, fill_drop / self.fill_drop_pct)

        # conservative aggregation: weighted average
        erosion_score = float((s_slip + s_imp + s_fill) / 3.0)

        self._record_audit(
            "evaluate",
            {
                "strategy": strategy,
                "samples": samples,
                "slippage_inc": slippage_inc,
                "impact_inc": impact_inc,
                "fill_drop": fill_drop,
                "erosion_score": erosion_score,
            },
        )

        if erosion_score <= 0.0 or erosion_score < self.min_erosion_score:
            return None

        # compute suggested capacity scaling
        reduction = erosion_score * self.max_reduction
        current = self.get_capacity(strategy)
        suggested = None
        if current is not None:
            suggested = max(0.0, current * (1.0 - reduction))

        cnt = self._counts.get(strategy, 0) + 1
        self._counts[strategy] = cnt
        severity = "warning" if cnt < self.escalate_count else "critical"
        ts = self._now_iso()
        alert = ErosionAlert(
            timestamp=ts,
            strategy=strategy,
            slippage_baseline=sb,
            slippage_recent=sr,
            impact_baseline=ib,
            impact_recent=ir,
            fill_baseline=fb,
            fill_recent=fr,
            erosion_score=erosion_score,
            suggested_capacity=suggested,
            severity=severity,
        )
        alert.evidence_hash = canonical_hash(
            {
                "ts": ts,
                "strategy": strategy,
                "erosion_score": erosion_score,
                "suggested": suggested,
                "severity": severity,
            }
        )
        self.alerts.append(alert)
        self._record_audit(
            "erosion_alert",
            {
                "strategy": strategy,
                "erosion_score": erosion_score,
                "suggested": suggested,
                "severity": severity,
            },
        )
        return alert


def _var(xs: List[float], ddof: int = 0) -> float:
    if not xs:
        return 0.0
    m = statistics.mean(xs)
    s = sum((x - m) ** 2 for x in xs)
    denom = len(xs) - ddof
    return s / denom if denom > 0 else 0.0
