import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class ChangeRequest:
    id: str
    title: str
    description: str
    proposer: str
    created_ts: str
    approved: bool = False
    approver: Optional[str] = None
    approved_ts: Optional[str] = None
    emergency: bool = False
    emergency_actor: Optional[str] = None
    emergency_justification: Optional[str] = None
    release_tag: Optional[str] = None
    rollback_plan: Optional[str] = None


class ChangeManagement:
    """Manage change requests, approvals, release tagging and rollback linkage.

    Hard rules:
    - No production change without approval (unless emergency override).
    - Emergency changes tracked separately (logged with justification).
    - Rollback plans required for approvals.
    """

    def __init__(self):
        self._store: Dict[str, ChangeRequest] = {}
        self.audit_log: List[Dict[str, Any]] = []

    def _log(
        self, action: str, details: Dict[str, Any], actor: Optional[str] = None
    ) -> None:
        self.audit_log.append(
            {"ts": _now_iso(), "actor": actor, "action": action, "details": details}
        )

    def create_request(self, title: str, description: str, proposer: str) -> str:
        rid = str(uuid.uuid4())
        cr = ChangeRequest(
            id=rid,
            title=title,
            description=description,
            proposer=proposer,
            created_ts=_now_iso(),
        )
        self._store[rid] = cr
        self._log("change_created", {"id": rid, "title": title}, proposer)
        return rid

    def approve_request(
        self,
        request_id: str,
        approver: str,
        rollback_plan: str,
        release_tag: Optional[str] = None,
    ) -> None:
        if request_id not in self._store:
            raise KeyError("unknown request")
        if not rollback_plan:
            raise ValueError("rollback_plan required for approval")
        cr = self._store[request_id]
        cr.approved = True
        cr.approver = approver
        cr.approved_ts = _now_iso()
        cr.rollback_plan = rollback_plan
        cr.release_tag = release_tag
        self._log(
            "change_approved",
            {
                "id": request_id,
                "rollback_plan": rollback_plan,
                "release_tag": release_tag,
            },
            approver,
        )

    def emergency_override(
        self, request_id: str, actor: str, justification: str
    ) -> None:
        if request_id not in self._store:
            raise KeyError("unknown request")
        cr = self._store[request_id]
        cr.emergency = True
        cr.emergency_actor = actor
        cr.emergency_justification = justification
        self._log(
            "emergency_override",
            {"id": request_id, "justification": justification},
            actor,
        )

    def tag_release(self, request_id: str, tag: str, actor: str) -> None:
        if request_id not in self._store:
            raise KeyError("unknown request")
        cr = self._store[request_id]
        cr.release_tag = tag
        self._log("release_tagged", {"id": request_id, "tag": tag}, actor)

    def link_rollback(self, request_id: str, rollback_plan: str, actor: str) -> None:
        if request_id not in self._store:
            raise KeyError("unknown request")
        cr = self._store[request_id]
        cr.rollback_plan = rollback_plan
        self._log(
            "rollback_linked", {"id": request_id, "rollback_plan": rollback_plan}, actor
        )

    def apply_change(self, request_id: str, actor: str) -> None:
        """Attempt to apply a change to production. Enforces approval unless emergency override present."""
        if request_id not in self._store:
            raise KeyError("unknown request")
        cr = self._store[request_id]
        if not cr.approved and not cr.emergency:
            self._log("apply_blocked_unapproved", {"id": request_id}, actor)
            raise RuntimeError("change not approved for production")
        # require rollback plan for non-emergency approved changes
        if cr.approved and not cr.rollback_plan:
            self._log("apply_blocked_no_rollback", {"id": request_id}, actor)
            raise RuntimeError("rollback plan required for approved change")
        self._log(
            "change_applied",
            {"id": request_id, "approved": cr.approved, "emergency": cr.emergency},
            actor,
        )

    def get_request(self, request_id: str) -> ChangeRequest:
        return self._store[request_id]
