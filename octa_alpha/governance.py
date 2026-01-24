from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List


@dataclass
class Governance:
    # states: 'pending', 'approved', 'vetoed'
    states: Dict[str, str] = field(default_factory=dict)
    audit_log: List[Dict[str, Any]] = field(default_factory=list)

    def _log(
        self,
        action: str,
        alpha_id: str,
        actor: str = "governance",
        reason: str = "",
        extra: Dict[str, Any] | None = None,
    ):
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "alpha_id": alpha_id,
            "actor": actor,
            "reason": reason,
            "extra": extra or {},
        }
        self.audit_log.append(event)
        return event

    def submit_for_approval(self, alpha_id: str, submitter: str = "author") -> None:
        self.states[alpha_id] = "pending"
        self._log("submit", alpha_id, actor=submitter)

    def approve(self, alpha_id: str, approver: str = "governance") -> None:
        self.states[alpha_id] = "approved"
        self._log("approve", alpha_id, actor=approver)

    def veto(self, alpha_id: str, vetoer: str = "governance", reason: str = "") -> None:
        self.states[alpha_id] = "vetoed"
        self._log("veto", alpha_id, actor=vetoer, reason=reason)

    def override_veto(
        self, alpha_id: str, actor: str = "governance_override", reason: str = ""
    ) -> None:
        # override moves to approved but logs override reason
        self.states[alpha_id] = "approved"
        self._log("override_veto", alpha_id, actor=actor, reason=reason)

    def is_approved(self, alpha_id: str) -> bool:
        return self.states.get(alpha_id) == "approved"

    def is_vetoed(self, alpha_id: str) -> bool:
        return self.states.get(alpha_id) == "vetoed"

    def get_audit(self) -> List[Dict[str, Any]]:
        return list(self.audit_log)


__all__ = ["Governance"]
