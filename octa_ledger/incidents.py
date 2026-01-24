from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .events import AuditEvent
from .store import LedgerStore


@dataclass(frozen=True)
class Incident:
    incident_id: str
    incident_type: str
    severity: int
    title: str
    created_at: str


class IncidentStore:
    """Create immutable, audited incidents backed by the ledger.

    Design:
    - Creating an incident writes an `incident.created` AuditEvent.
    - Timeline entries, root cause notes and resolution are separate AuditEvents
      referencing `incident_id` (action names: `incident.timeline`, `incident.root_cause`, `incident.resolved`).
    - Incidents are immutable by design: state transitions are represented by new AuditEvents.
    """

    def __init__(self, ledger: LedgerStore) -> None:
        self.ledger = ledger

    def create_incident(
        self,
        incident_type: str,
        severity: int,
        title: str,
        initial_notes: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Incident:
        payload: Dict[str, Any] = {
            "type": incident_type,
            "severity": int(severity),
            "title": title,
            "notes": initial_notes or "",
            "context": context or {},
        }
        ev = AuditEvent.create(
            actor="incidents",
            action="incident.created",
            payload=payload,
            severity="ERROR",
        )
        self.ledger.append(ev)
        return Incident(
            incident_id=ev.event_id,
            incident_type=incident_type,
            severity=severity,
            title=title,
            created_at=ev.timestamp,
        )

    def append_timeline(
        self,
        incident_id: str,
        note: str,
        actor: str = "incidents",
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload = {"incident_id": incident_id, "note": note, "meta": meta or {}}
        ev = AuditEvent.create(
            actor=actor, action="incident.timeline", payload=payload, severity="WARN"
        )
        self.ledger.append(ev)

    def add_root_cause(
        self, incident_id: str, root_cause: str, automated: bool = False
    ) -> None:
        payload = {
            "incident_id": incident_id,
            "root_cause": root_cause,
            "automated": bool(automated),
        }
        ev = AuditEvent.create(
            actor="incidents",
            action="incident.root_cause",
            payload=payload,
            severity="INFO",
        )
        self.ledger.append(ev)

    def resolve_incident(
        self, incident_id: str, resolution_notes: str, resolved_by: str = "operator"
    ) -> None:
        payload = {
            "incident_id": incident_id,
            "resolution_notes": resolution_notes,
            "resolved_by": resolved_by,
        }
        ev = AuditEvent.create(
            actor=resolved_by,
            action="incident.resolved",
            payload=payload,
            severity="INFO",
        )
        self.ledger.append(ev)


__all__ = ["IncidentStore", "Incident"]
