from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(obj: Any) -> str:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False
    )


def _evidence_hash(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()


@dataclass
class SuccessionPlan:
    system: str
    owners: List[str] = field(default_factory=list)
    trustees: List[str] = field(default_factory=list)
    custodians: List[str] = field(default_factory=list)
    emergency_custodians: Dict[str, str] = field(default_factory=dict)
    transitions: List[Dict] = field(default_factory=list)
    created_at: str = field(default_factory=_now_iso)

    def to_manifest(self) -> Dict:
        return asdict(self)


class SuccessionError(Exception):
    pass


class SuccessionManager:
    """Succession and long‑term maintenance manager.

    Enforces:
    - No key‑person dependency: require minimum owner/trustee redundancy.
    - Governance continuity: scheduled transitions must have approvers.
    - Emergency custodianship: emergency triggers and custodial handover.
    """

    def __init__(
        self, plan: Optional[SuccessionPlan] = None, min_owner_count: int = 2
    ) -> None:
        self.plan = plan or SuccessionPlan(system="octa_core")
        self.min_owner_count = max(1, min_owner_count)
        self.audit: List[Dict] = []

    def register_owner(self, owner: str) -> None:
        if owner in self.plan.owners:
            return
        self.plan.owners.append(owner)
        self._record_event("register_owner", {"owner": owner})

    def remove_owner(self, owner: str) -> None:
        if owner not in self.plan.owners:
            raise SuccessionError("owner not found")
        if len(self.plan.owners) - 1 < self.min_owner_count:
            raise SuccessionError("removal would violate min_owner_count")
        self.plan.owners.remove(owner)
        self._record_event("remove_owner", {"owner": owner})

    def add_trustee(self, trustee: str) -> None:
        if trustee in self.plan.trustees:
            return
        self.plan.trustees.append(trustee)
        self._record_event("add_trustee", {"trustee": trustee})

    def designate_emergency_custodian(self, role: str, name: str) -> None:
        self.plan.emergency_custodians[role] = name
        self._record_event(
            "designate_emergency_custodian", {"role": role, "name": name}
        )

    def schedule_transition(
        self, when_iso: str, from_party: str, to_party: str, approvers: List[str]
    ) -> None:
        # simple validation
        if from_party not in (
            self.plan.owners + self.plan.trustees + self.plan.custodians
        ):
            raise SuccessionError("from_party not recognized")
        if not approvers:
            raise SuccessionError("approvers required for governance continuity")
        transition = {
            "when": when_iso,
            "from": from_party,
            "to": to_party,
            "approvers": approvers,
            "scheduled_at": _now_iso(),
        }
        self.plan.transitions.append(transition)
        self._record_event("schedule_transition", transition)

    def execute_transition(self, transition_idx: int) -> None:
        try:
            t = self.plan.transitions[transition_idx]
        except IndexError:
            raise SuccessionError("unknown transition") from None
        # enforce approvers present
        if not t.get("approvers"):
            raise SuccessionError("transition lacks approvers")
        # perform a minimal transfer: remove from owners if present, add to owners
        from_p = t.get("from")
        to_p = t.get("to")
        if isinstance(from_p, str) and from_p in self.plan.owners:
            if len(self.plan.owners) - 1 < self.min_owner_count:
                raise SuccessionError(
                    "cannot execute transition; would break min_owner_count"
                )
            self.plan.owners.remove(from_p)
        if isinstance(to_p, str) and to_p not in self.plan.owners:
            self.plan.owners.append(to_p)
        t["executed_at"] = _now_iso()
        self._record_event("execute_transition", t)

    def trigger_emergency(
        self, role: str, reason: str, evidence: Optional[Dict] = None
    ) -> Dict:
        # locate emergency custodian
        cust = self.plan.emergency_custodians.get(role)
        if not cust:
            raise SuccessionError("no emergency custodian designated for role")
        ev = {
            "role": role,
            "custodian": cust,
            "reason": reason,
            "evidence": evidence or {},
            "triggered_at": _now_iso(),
        }
        self._record_event("trigger_emergency", ev)
        # return custodial handover manifest (must be audited & timeboxed)
        manifest = {
            "custodian": cust,
            "role": role,
            "handover_at": _now_iso(),
            "timebox_days": 30,
        }
        return manifest

    def verify_no_key_person_dependency(self) -> bool:
        # True if number of owners >= min_owner_count and at least two trustees or trustees+owners redundancy
        if len(self.plan.owners) < self.min_owner_count:
            return False
        if len(self.plan.trustees) + len(self.plan.owners) < (self.min_owner_count + 1):
            return False
        return True

    def export_manifest(self) -> Dict:
        manifest = self.plan.to_manifest()
        manifest["evidence_hash"] = _evidence_hash(manifest)
        self._record_event(
            "export_manifest", {"evidence_hash": manifest["evidence_hash"]}
        )
        return manifest

    def _record_event(self, action: str, payload: Dict) -> None:
        entry = {"action": action, "payload": payload, "ts": _now_iso()}
        entry["hash"] = _evidence_hash(entry)
        self.audit.append(entry)

    def list_audit(self) -> List[Dict]:
        # defensive copy
        return json.loads(_canonical(self.audit))  # type: ignore
