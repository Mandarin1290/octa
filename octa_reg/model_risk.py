import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class ModelEntry:
    id: str
    name: str
    version: str
    metadata: Dict[str, Any]
    approved: bool = False
    approved_by: Optional[str] = None
    approved_ts: Optional[str] = None
    validation_evidence: List[Dict[str, Any]] = field(default_factory=list)
    created_ts: str = field(default_factory=_now_iso)


class ModelRiskManager:
    """Manage model inventory, approval workflow, validation evidence and override logging.

    Hard rules enforced:
    - Every model must have an approval state before production use.
    - Model changes require validation evidence; approvals are audited.
    - Production use without approval is forbidden unless explicitly overridden (and overrides are logged).
    """

    def __init__(self):
        self.models: Dict[str, ModelEntry] = {}
        self.audit_log: List[Dict[str, Any]] = []

    def _log(
        self, action: str, details: Dict[str, Any], actor: Optional[str] = None
    ) -> None:
        self.audit_log.append(
            {"ts": _now_iso(), "actor": actor, "action": action, "details": details}
        )

    def register_model(
        self, name: str, version: str, metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        mid = str(uuid.uuid4())
        entry = ModelEntry(id=mid, name=name, version=version, metadata=metadata or {})
        self.models[mid] = entry
        self._log(
            "model_registered", {"id": mid, "name": name, "version": version}, None
        )
        return mid

    def propose_change(
        self, model_id: str, new_version: str, actor: Optional[str] = None
    ) -> None:
        if model_id not in self.models:
            raise KeyError("unknown model")
        m = self.models[model_id]
        # require validation before switching approved flag
        m.version = new_version
        m.approved = False
        m.approved_by = None
        m.approved_ts = None
        m.validation_evidence = []
        self._log(
            "model_change_proposed", {"id": model_id, "new_version": new_version}, actor
        )

    def add_validation_evidence(
        self, model_id: str, evidence: Dict[str, Any], actor: Optional[str] = None
    ) -> None:
        if model_id not in self.models:
            raise KeyError("unknown model")
        self.models[model_id].validation_evidence.append(evidence)
        self._log(
            "validation_evidence_added", {"id": model_id, "evidence": evidence}, actor
        )

    def approve_model(self, model_id: str, approver: str) -> None:
        if model_id not in self.models:
            raise KeyError("unknown model")
        m = self.models[model_id]
        # require at least one validation evidence
        if not m.validation_evidence:
            raise RuntimeError("model cannot be approved without validation evidence")
        m.approved = True
        m.approved_by = approver
        m.approved_ts = _now_iso()
        self._log("model_approved", {"id": model_id, "approver": approver}, approver)

    def reject_model(self, model_id: str, approver: str, reason: str) -> None:
        if model_id not in self.models:
            raise KeyError("unknown model")
        m = self.models[model_id]
        m.approved = False
        m.approved_by = None
        m.approved_ts = None
        self._log(
            "model_rejected",
            {"id": model_id, "approver": approver, "reason": reason},
            approver,
        )

    def use_model(self, model_id: str, actor: str) -> None:
        """Mark attempt to use model in production; enforce approval unless overridden."""
        if model_id not in self.models:
            raise KeyError("unknown model")
        m = self.models[model_id]
        if not m.approved:
            self._log("use_blocked_unapproved", {"id": model_id}, actor)
            raise RuntimeError("model use blocked: model not approved for production")
        self._log("model_used", {"id": model_id, "version": m.version}, actor)

    def override_use(self, model_id: str, actor: str, justification: str) -> None:
        """Allow temporary override of production use for an unapproved model — must be logged."""
        if model_id not in self.models:
            raise KeyError("unknown model")
        m = self.models[model_id]
        # log override event; do not change approval state
        self._log(
            "model_use_overridden",
            {"id": model_id, "justification": justification, "version": m.version},
            actor,
        )

    def list_models(self) -> List[ModelEntry]:
        return sorted(self.models.values(), key=lambda m: (m.name, m.version, m.id))
