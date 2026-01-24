import hashlib
import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class LRAlert:
    timestamp: str
    metric: str
    severity: str
    value: float
    threshold: float
    details: Dict[str, Any]
    evidence_hash: str = ""


class LongRunStabilityMonitor:
    """Detect slow degradation across memory, latency, execution variance and error-rate drift.

    Approach:
    - For each metric maintain an ordered sample history.
    - Compute short-term and long-term averages; trigger when short-term exceeds long-term by ratio threshold.
    - Escalate alerts after repeated triggers for the same metric.
    """

    def __init__(
        self,
        long_window: int = 60,
        short_window: int = 10,
        ratio_threshold: float = 1.2,
        escalate_count: int = 3,
    ):
        self.long_window = int(long_window)
        self.short_window = int(short_window)
        self.ratio_threshold = float(ratio_threshold)
        self.escalate_count = int(escalate_count)

        # histories: metric -> list of floats
        self.histories: Dict[str, List[float]] = {
            "memory": [],
            "latency": [],
            "exec_time": [],
            "error_rate": [],
        }
        self.alerts: List[LRAlert] = []
        self._counts: Dict[str, int] = {}
        self.audit_log: List[Dict[str, Any]] = []

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _record_audit(self, action: str, details: Dict[str, Any]):
        ts = self._now_iso()
        rec = {"ts": ts, "action": action, "details": details}
        rec["evidence_hash"] = canonical_hash(rec)
        self.audit_log.append(rec)

    def _sample(self, metric: str, value: float):
        if metric not in self.histories:
            raise ValueError("unknown metric")
        self.histories[metric].append(float(value))
        # cap history length
        max_len = max(self.long_window, self.short_window) * 2
        if len(self.histories[metric]) > max_len:
            self.histories[metric] = self.histories[metric][-max_len:]
        self._record_audit("sample", {"metric": metric, "value": value})

    def record_memory(self, bytes_used: float):
        # record in MB
        self._sample("memory", bytes_used / (1024.0 * 1024.0))

    def record_latency(self, ms: float):
        self._sample("latency", ms)

    def record_execution_time(self, ms: float):
        self._sample("exec_time", ms)

    def record_error(self, errors_per_minute: float):
        self._sample("error_rate", errors_per_minute)

    def _moving_avg(self, data: List[float], n: int) -> float:
        if not data:
            return 0.0
        if len(data) < n:
            return statistics.mean(data)
        return statistics.mean(data[-n:])

    def evaluate(self) -> List[LRAlert]:
        triggered: List[LRAlert] = []
        for metric, data in self.histories.items():
            if len(data) < 2:
                continue
            long_avg = self._moving_avg(data, min(len(data), self.long_window))
            short_avg = self._moving_avg(data, min(len(data), self.short_window))
            # avoid divide by zero
            if long_avg <= 0:
                continue
            ratio = (short_avg / long_avg) if long_avg > 0 else float("inf")
            self._record_audit(
                "evaluate",
                {
                    "metric": metric,
                    "short_avg": short_avg,
                    "long_avg": long_avg,
                    "ratio": ratio,
                },
            )
            if ratio >= self.ratio_threshold:
                cnt = self._counts.get(metric, 0) + 1
                self._counts[metric] = cnt
                severity = "warning" if cnt < self.escalate_count else "critical"
                ts = self._now_iso()
                alert = LRAlert(
                    timestamp=ts,
                    metric=metric,
                    severity=severity,
                    value=short_avg,
                    threshold=self.ratio_threshold,
                    details={
                        "short_avg": short_avg,
                        "long_avg": long_avg,
                        "ratio": ratio,
                    },
                )
                alert.evidence_hash = canonical_hash(
                    {
                        "ts": ts,
                        "metric": metric,
                        "severity": severity,
                        "value": short_avg,
                        "threshold": self.ratio_threshold,
                        "details": alert.details,
                    }
                )
                self.alerts.append(alert)
                triggered.append(alert)
                self._record_audit(
                    "alert", {"metric": metric, "severity": severity, "ratio": ratio}
                )
        return triggered

    def get_alerts(self, severity: str | None = None) -> List[LRAlert]:
        if severity is None:
            return list(self.alerts)
        return [a for a in self.alerts if a.severity == severity]
