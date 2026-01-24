from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class KillSwitch:
    engaged: bool = False
    engaged_by: Optional[str] = None
    engaged_at: Optional[str] = None
    reason: Optional[str] = None
    cleared: bool = False
    cleared_by: Optional[str] = None
    audit_log: list = field(default_factory=list)
    clear_roles: tuple = ("admin",)

    def _log(self, actor: str, action: str, details: Optional[Dict[str, Any]] = None):
        self.audit_log.append(
            {
                "ts": _now_iso(),
                "actor": actor,
                "action": action,
                "details": details or {},
            }
        )

    def engage(self, actor: str, reason: str, automated: bool = False) -> bool:
        if self.engaged:
            # already engaged
            self._log(
                actor, "engage_ignored", {"reason": reason, "automated": automated}
            )
            return False
        self.engaged = True
        self.engaged_by = actor
        self.engaged_at = _now_iso()
        self.reason = reason
        self.cleared = False
        self.cleared_by = None
        self._log(actor, "engaged", {"reason": reason, "automated": automated})
        return True

    def clear(self, actor_role: str, actor: str) -> bool:
        # only permitted roles can clear
        if actor_role not in self.clear_roles:
            self._log(actor, "clear_denied", {"role": actor_role})
            return False
        if not self.engaged:
            self._log(actor, "clear_noop", {})
            return False
        self.engaged = False
        self.cleared = True
        self.cleared_by = actor
        self._log(actor, "cleared", {})
        return True

    def is_engaged(self) -> bool:
        return self.engaged

    def flatten_portfolio(self, oms) -> None:
        # override all modules by forcing positions to zero in provided OMS-like object
        try:
            # copy keys to avoid mutation during iteration
            strategies = list(oms._positions.keys())
            for s in strategies:
                oms._positions[s] = {
                    sym: 0.0 for sym in oms._positions.get(s, {}).keys()
                }
            # record audit in OMS if available
            if hasattr(oms, "_log"):
                oms._log(
                    "kill_switch",
                    "flatten",
                    {"by": self.engaged_by, "reason": self.reason},
                )
        except Exception:
            # best-effort: set generic attribute if present
            if hasattr(oms, "_positions"):
                oms._positions = {}
        self._log("kill_switch", "flatten_called", {"by": self.engaged_by})


kill_switch = KillSwitch()

__all__ = ["KillSwitch", "kill_switch"]
