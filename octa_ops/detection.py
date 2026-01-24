from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from octa_ops.incidents import IncidentManager


@dataclass
class Signal:
    component: str
    signal_type: str
    ts: str  # ISO timestamp
    details: Dict[str, Any]
    weight: int = 0


class DetectionEngine:
    """Rule-based detection aggregator that converts signals into incidents.

    Rules (conservative, deterministic):
    - Each signal type has a default weight.
    - Immediate incident if a single signal weight >= IMMEDIATE_WEIGHT_THRESHOLD.
    - Otherwise, sum weights within a sliding window (`window_seconds`).
      - If total_weight >= 11 -> classify by `IncidentManager.classify_from_impact` and create incident.
      - If total_weight in 1..10 and count_signals >= 3 -> escalate (treat as >=11 for classification).
    - Single weak signals (noise) are ignored.
    - All signals must include a timestamp.
    """

    DEFAULT_WEIGHTS = {
        "execution_error": 100,
        "latency_spike": 5,
        "risk_gate_violation": 80,
        "data_feed_anomaly": 30,
        "broker_api_failure": 70,
    }

    IMMEDIATE_WEIGHT_THRESHOLD = 80

    def __init__(self, incident_manager: IncidentManager, window_seconds: int = 300):
        self.im = incident_manager
        self.window = timedelta(seconds=window_seconds)
        self._signals: List[Signal] = []

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def ingest(
        self,
        component: str,
        signal_type: str,
        ts: str | None = None,
        details: Dict[str, Any] | None = None,
        weight: int | None = None,
    ) -> None:
        if ts is None:
            ts = self._now().isoformat()
        if details is None:
            details = {}
        if weight is None:
            weight = int(self.DEFAULT_WEIGHTS.get(signal_type, 1))
        sig = Signal(
            component=component,
            signal_type=signal_type,
            ts=ts,
            details=details,
            weight=weight,
        )
        self._signals.append(sig)

    def _parse_ts(self, s: str) -> datetime:
        return datetime.fromisoformat(s)

    def evaluate(self) -> List[Dict[str, Any]]:
        """Evaluate signals and create incidents where rules trigger.

        Returns list of created incidents' dicts.
        """
        created = []
        now = self._now()

        # group signals by component and filter window
        by_comp: Dict[str, List[Signal]] = {}
        cutoff = now - self.window
        for s in self._signals:
            try:
                ts_dt = self._parse_ts(s.ts)
            except Exception:
                # malformed timestamp — treat as now for conservatism
                ts_dt = now
            if ts_dt < cutoff:
                continue
            by_comp.setdefault(s.component, []).append(s)

        # deterministic processing: sort components
        for comp in sorted(by_comp.keys()):
            sigs = sorted(by_comp[comp], key=lambda x: (x.ts, x.signal_type))

            # check immediate severe signals
            for s in sigs:
                if s.weight >= self.IMMEDIATE_WEIGHT_THRESHOLD:
                    sev = self.im.classify_from_impact(s.weight)
                    inc = self.im.record_incident(
                        title=f"Immediate incident: {comp} {s.signal_type}",
                        description=f"Triggered by single signal: {s.signal_type}",
                        reporter="detection_engine",
                        severity=sev,
                        metadata={"signals": [asdict(s)]},
                    )
                    created.append(
                        {"incident_id": inc.id, "severity": sev.name, "component": comp}
                    )
                    # once immediate incident created for component, skip further processing for this window
                    sigs = []
                    break

            if not sigs:
                continue

            total_weight = sum(s.weight for s in sigs)
            count = len(sigs)

            # rule: multiple weak signals can escalate
            adjusted_weight = total_weight
            if 1 <= total_weight <= 10 and count >= 3:
                adjusted_weight = 11

            if adjusted_weight >= 11:
                sev = self.im.classify_from_impact(adjusted_weight)
                inc = self.im.record_incident(
                    title=f"Aggregated incident: {comp}",
                    description=f"Aggregated {count} signals totaling weight {adjusted_weight}",
                    reporter="detection_engine",
                    severity=sev,
                    metadata={
                        "signal_count": count,
                        "total_weight": adjusted_weight,
                        "signals": [asdict(s) for s in sigs],
                    },
                )
                created.append(
                    {"incident_id": inc.id, "severity": sev.name, "component": comp}
                )

        return created


__all__ = ["DetectionEngine", "Signal"]
