import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()


@dataclass
class AuditEntry:
    ts: str
    actor: str
    action: str
    details: Dict[str, Any]


@dataclass
class ConfigStore:
    data: Dict[str, Any] = field(default_factory=dict)
    audit: List[AuditEntry] = field(default_factory=list)

    def apply_change(self, actor: str, key: str, value: Any):
        old = self.data.get(key)
        self.data[key] = value
        self.audit.append(
            AuditEntry(
                ts=_now_iso(),
                actor=actor,
                action="config_change",
                details={"key": key, "old": old, "new": value},
            )
        )


@dataclass
class User:
    id: str
    roles: List[str]


class PrivilegeManager:
    """Enforce least-privilege, log attempts, and provide survivable defaults.

    Roles are strings: 'ops', 'admin', 'risk', 'trader'. Policies are simple mappings for this simulation.
    """

    def __init__(self, config: ConfigStore):
        self.config = config
        self.audit: List[Dict[str, Any]] = []
        # simple policy map: action -> allowed roles
        self.policies: Dict[str, List[str]] = {
            "change_config": ["admin", "ops"],
            "bypass_risk": ["admin"],
            "reallocate_capital": ["admin", "ops"],
        }

    def _log(
        self,
        actor: str,
        action: str,
        allowed: bool,
        details: Optional[Dict[str, Any]] = None,
    ):
        entry = {
            "ts": _now_iso(),
            "actor": actor,
            "action": action,
            "allowed": allowed,
            "details": details or {},
        }
        self.audit.append(entry)

    def check_permission(self, user: User, action: str) -> bool:
        allowed_roles = self.policies.get(action, [])
        allowed = any(r in allowed_roles for r in user.roles)
        self._log(user.id, action + ".check", allowed, {"user_roles": user.roles})
        return allowed

    def attempt_change_config(self, user: User, key: str, value: Any) -> bool:
        if not self.check_permission(user, "change_config"):
            self._log(
                user.id, "change_config.denied", False, {"key": key, "value": value}
            )
            return False
        # apply change in a controlled manner
        self.config.apply_change(user.id, key, value)
        self._log(user.id, "change_config.applied", True, {"key": key, "value": value})
        return True

    def attempt_bypass_risk(self, user: User, risk_gate_id: str) -> bool:
        if not self.check_permission(user, "bypass_risk"):
            self._log(user.id, "bypass_risk.denied", False, {"risk_gate": risk_gate_id})
            return False
        # record bypass action but do not change system state in this simulation
        self._log(user.id, "bypass_risk.applied", True, {"risk_gate": risk_gate_id})
        return True

    def attempt_reallocate(
        self, user: User, from_account: str, to_account: str, amount: float
    ) -> bool:
        if not self.check_permission(user, "reallocate_capital"):
            self._log(
                user.id,
                "reallocate.denied",
                False,
                {"from": from_account, "to": to_account, "amount": amount},
            )
            return False
        # in simulation, update config store to reflect transfer ledger
        ledger = self.config.data.setdefault("capital_ledger", {})
        ledger.setdefault(from_account, 0.0)
        ledger.setdefault(to_account, 0.0)
        ledger[from_account] = max(0.0, ledger[from_account] - amount)
        ledger[to_account] = ledger.get(to_account, 0.0) + amount
        self.config.audit.append(
            AuditEntry(
                ts=_now_iso(),
                actor=user.id,
                action="reallocate",
                details={"from": from_account, "to": to_account, "amount": amount},
            )
        )
        self._log(
            user.id,
            "reallocate.applied",
            True,
            {"from": from_account, "to": to_account, "amount": amount},
        )
        return True


__all__ = ["PrivilegeManager", "ConfigStore", "User", "AuditEntry"]
