from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


class EscalationException(Exception):
    pass


@dataclass
class Escalation:
    id: str
    type: str
    details: Dict[str, Any]
    created_by: str
    created_at: datetime.datetime
    status: str = "open"
    approvals: List[Dict[str, Any]] = field(default_factory=list)
    required_approval_count: int = 2
    required_roles: Optional[Set[str]] = None

    def add_approval(self, actor: str, role: str) -> None:
        # prevent duplicate approvals by same actor
        if any(a["actor"] == actor for a in self.approvals):
            return
        self.approvals.append(
            {
                "actor": actor,
                "role": role,
                "ts": datetime.datetime.utcnow().isoformat() + "Z",
            }
        )

    def approval_count(self) -> int:
        return len({a["actor"] for a in self.approvals})

    def roles_approved(self) -> Set[str]:
        return {a["role"] for a in self.approvals}


class EscalationManager:
    def __init__(self):
        self._store: Dict[str, Escalation] = {}
        self.audit_log: List[Dict[str, Any]] = []

    def _record_audit(self, actor: str, action: str, details: Dict[str, Any]) -> None:
        self.audit_log.append(
            {
                "ts": datetime.datetime.utcnow().isoformat() + "Z",
                "actor": actor,
                "action": action,
                "details": details,
            }
        )

    def trigger_escalation(
        self,
        esc_type: str,
        details: Dict[str, Any],
        created_by: str,
        required_approval_count: int = 2,
        required_roles: Optional[Set[str]] = None,
    ) -> Escalation:
        eid = str(uuid.uuid4())
        esc = Escalation(
            id=eid,
            type=esc_type,
            details=details,
            created_by=created_by,
            created_at=datetime.datetime.utcnow(),
            required_approval_count=required_approval_count,
            required_roles=required_roles,
        )
        self._store[eid] = esc
        self._record_audit(
            created_by,
            "escalation_triggered",
            {
                "id": eid,
                "type": esc_type,
                "required_approval_count": required_approval_count,
                "required_roles": list(required_roles) if required_roles else None,
                "details": details,
            },
        )
        return esc

    def add_approval(self, esc_id: str, actor: str, role: str) -> None:
        if esc_id not in self._store:
            raise EscalationException("escalation_not_found")
        esc = self._store[esc_id]
        esc.add_approval(actor, role)
        self._record_audit(actor, "escalation_approval", {"id": esc_id, "role": role})

    def resolve_escalation(self, esc_id: str, resolved_by: str) -> None:
        if esc_id not in self._store:
            raise EscalationException("escalation_not_found")
        esc = self._store[esc_id]

        if esc.status != "open":
            raise EscalationException("escalation_not_open")

        # Enforce no single-actor absolute control: require at least required_approval_count distinct approvers
        if esc.approval_count() < esc.required_approval_count:
            raise EscalationException("insufficient_approvals")

        # If specific roles are required, ensure they are covered
        if esc.required_roles:
            if not esc.required_roles.issubset(esc.roles_approved()):
                raise EscalationException("required_roles_missing")

        esc.status = "resolved"
        self._record_audit(
            resolved_by,
            "escalation_resolved",
            {"id": esc_id, "resolved_by": resolved_by, "approvals": esc.approvals},
        )

    def get(self, esc_id: str) -> Escalation:
        return self._store[esc_id]
