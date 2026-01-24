import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List


def canonical_hash(obj) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class Alert:
    timestamp: str
    metric: str
    value: float
    threshold: float
    severity: str
    details: Dict[str, Any]
    evidence_hash: str = ""


class LiveCapitalMonitor:
    """Monitor NAV drift, fee accrual and exposure vs capital, with alert escalation.

    Escalation policy:
    - First trigger -> 'warning'
    - Repeated triggers for same metric (count >= escalate_count) -> 'critical'
    """

    def __init__(
        self,
        nav_drift_threshold: float = 0.05,
        fee_accrual_threshold: float = 10000.0,
        exposure_percent_threshold: float = 0.5,
        escalate_count: int = 3,
    ):
        self.nav_drift_threshold = float(nav_drift_threshold)
        self.fee_accrual_threshold = float(fee_accrual_threshold)
        self.exposure_percent_threshold = float(exposure_percent_threshold)
        self.escalate_count = int(escalate_count)

        self.alerts: List[Alert] = []
        self.audit_log: List[Dict[str, Any]] = []
        self._trigger_counts: Dict[str, int] = {}

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _record_audit(self, action: str, details: Dict[str, Any]):
        ts = self._now_iso()
        rec = {"ts": ts, "action": action, "details": details}
        rec["evidence_hash"] = canonical_hash(rec)
        self.audit_log.append(rec)

    def _emit_alert(
        self, metric: str, value: float, threshold: float, details: Dict[str, Any]
    ):
        cnt = self._trigger_counts.get(metric, 0) + 1
        self._trigger_counts[metric] = cnt
        severity = "warning" if cnt < self.escalate_count else "critical"
        ts = self._now_iso()
        alert = Alert(
            timestamp=ts,
            metric=metric,
            value=value,
            threshold=threshold,
            severity=severity,
            details=details,
        )
        alert.evidence_hash = canonical_hash(
            {
                "ts": ts,
                "metric": metric,
                "value": value,
                "threshold": threshold,
                "severity": severity,
                "details": details,
            }
        )
        self.alerts.append(alert)
        self._record_audit(
            "alert_emitted",
            {
                "metric": metric,
                "value": value,
                "threshold": threshold,
                "severity": severity,
                "details": details,
            },
        )
        return alert

    def record_nav(self, baseline_nav: float, current_nav: float) -> List[Alert]:
        """Check NAV drift relative to baseline (absolute fraction) and emit alert if drift exceeds threshold."""
        if baseline_nav <= 0:
            raise ValueError("baseline_nav must be positive")
        drift = abs(current_nav - baseline_nav) / baseline_nav
        self._record_audit(
            "nav_recorded",
            {"baseline_nav": baseline_nav, "current_nav": current_nav, "drift": drift},
        )
        if drift > self.nav_drift_threshold:
            a = self._emit_alert(
                "nav_drift",
                drift,
                self.nav_drift_threshold,
                {"baseline_nav": baseline_nav, "current_nav": current_nav},
            )
            return [a]
        return []

    def record_fee_accrual(self, accrued_amount: float) -> List[Alert]:
        self._record_audit("fee_accrued", {"accrued_amount": accrued_amount})
        if accrued_amount >= self.fee_accrual_threshold:
            a = self._emit_alert(
                "fee_accrual", accrued_amount, self.fee_accrual_threshold, {}
            )
            return [a]
        return []

    def record_exposure(self, exposure: float, capital: float) -> List[Alert]:
        if capital <= 0:
            raise ValueError("capital must be positive")
        pct = exposure / capital
        self._record_audit(
            "exposure_recorded", {"exposure": exposure, "capital": capital, "pct": pct}
        )
        if pct >= self.exposure_percent_threshold:
            a = self._emit_alert(
                "exposure_vs_capital",
                pct,
                self.exposure_percent_threshold,
                {"exposure": exposure, "capital": capital},
            )
            return [a]
        return []

    def get_alerts(self, severity: str | None = None) -> List[Alert]:
        if severity is None:
            return list(self.alerts)
        return [a for a in self.alerts if a.severity == severity]

    def snapshot(self) -> Dict[str, Any]:
        return {
            "nav_drift_threshold": self.nav_drift_threshold,
            "fee_accrual_threshold": self.fee_accrual_threshold,
            "exposure_percent_threshold": self.exposure_percent_threshold,
            "alerts_count": len(self.alerts),
        }
