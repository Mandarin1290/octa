import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List


class Severity(Enum):
    S0 = 0  # informational
    S1 = 1  # degraded performance
    S2 = 2  # trading impairment
    S3 = 3  # capital at risk
    S4 = 4  # existential threat


@dataclass
class Incident:
    id: str
    title: str
    description: str
    reporter: str
    ts: str
    severity: Severity
    metadata: Dict[str, Any] = field(default_factory=dict)


class IncidentManager:
    """Manage incidents, classification and escalation.

    Hard rules enforced:
    - Every incident must have a `Severity`.
    - Severity determines escalation levels and required permissions.
    - No silent failures: attempting to record an incident without severity raises.
    """

    ESCALATION_RULES = {
        Severity.S0: ["ops_team"],
        Severity.S1: ["ops_team", "oncall_engineer"],
        Severity.S2: ["ops_team", "oncall_engineer", "trading_desk_lead"],
        Severity.S3: [
            "ops_team",
            "oncall_engineer",
            "trading_desk_lead",
            "risk_officer",
        ],
        Severity.S4: ["exec_committee", "legal", "risk_officer", "ops_team"],
    }

    PERMISSIONS = {
        Severity.S0: ["read"],
        Severity.S1: ["read", "ack"],
        Severity.S2: ["read", "ack", "mitigate"],
        Severity.S3: ["read", "ack", "mitigate", "halt_trading"],
        Severity.S4: ["read", "ack", "mitigate", "halt_trading", "evacuate"],
    }

    def __init__(self):
        self._store: Dict[str, Incident] = {}

    @staticmethod
    def classify_from_impact(impact_score: int) -> Severity:
        """Deterministic mapping from an integer impact score to a Severity.

        Thresholds (conservative, deterministic):
        - 0 -> S0
        - 1-10 -> S1
        - 11-50 -> S2
        - 51-200 -> S3
        - >200 -> S4
        """

        if impact_score <= 0:
            return Severity.S0
        if impact_score <= 10:
            return Severity.S1
        if impact_score <= 50:
            return Severity.S2
        if impact_score <= 200:
            return Severity.S3
        return Severity.S4

    def record_incident(
        self,
        title: str,
        description: str,
        reporter: str,
        severity: Severity | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Incident:
        # Hard rule: every incident must have a severity
        if severity is None:
            raise ValueError(
                "Incident must have a severity; silent failures are not allowed"
            )

        if not isinstance(severity, Severity):
            raise TypeError("severity must be a Severity enum value")

        ts = datetime.now(timezone.utc).isoformat()
        iid = str(uuid.uuid4())
        inc = Incident(
            id=iid,
            title=title,
            description=description,
            reporter=reporter,
            ts=ts,
            severity=severity,
            metadata=metadata or {},
        )
        self._store[iid] = inc
        return inc

    def get_incident(self, incident_id: str) -> Incident:
        return self._store[incident_id]

    def list_incidents(self) -> List[Incident]:
        # deterministic ordering by timestamp then id
        return sorted(self._store.values(), key=lambda i: (i.ts, i.id))

    def escalation_for(self, severity: Severity) -> List[str]:
        return list(self.ESCALATION_RULES.get(severity, []))

    def permissions_for(self, severity: Severity) -> List[str]:
        return list(self.PERMISSIONS.get(severity, []))


__all__ = ["Severity", "Incident", "IncidentManager"]
