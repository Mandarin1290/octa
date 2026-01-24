import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List


def canonical_hash(obj) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class CutoverRecord:
    timestamp: str
    action: str
    details: Dict[str, Any]
    evidence_hash: str = ""


class CutoverError(Exception):
    pass


class IrreversibleError(CutoverError):
    pass


class CutoverManager:
    """Manage a one-way production cutover: paper -> shadow -> live.

    Hard rules:
    - One-way transition: cannot revert to earlier states.
    - No silent rollback: attempts to revert raise `IrreversibleError`.
    - Final checks must pass before moving to `LIVE`.
    """

    STATE_PAPER = "PAPER"
    STATE_SHADOW = "SHADOW"
    STATE_LIVE = "LIVE"

    def __init__(self):
        self.state = self.STATE_PAPER
        self.audit_log: List[CutoverRecord] = []
        self.final_checks_passed = False
        self.capital_unlocked = False

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _record(self, action: str, details: Dict[str, Any]):
        ts = self._now_iso()
        rec = CutoverRecord(timestamp=ts, action=action, details=details)
        rec.evidence_hash = canonical_hash(
            {"ts": ts, "action": action, "details": details}
        )
        self.audit_log.append(rec)

    def run_final_checks(
        self, check_fn: Callable[[], Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Run caller-provided final checks function which must return a dict with {'ok': bool, ...}.

        Stores result in audit log and sets `final_checks_passed` accordingly.
        """
        res = check_fn()
        if not isinstance(res, dict) or "ok" not in res:
            raise ValueError("check_fn must return a dict containing key 'ok'")
        self.final_checks_passed = bool(res.get("ok"))
        self._record("final_checks", res)
        return res

    def transition_to_shadow(self) -> str:
        if self.state != self.STATE_PAPER:
            raise CutoverError("Can only transition to SHADOW from PAPER")
        self.state = self.STATE_SHADOW
        self._record("transition", {"from": self.STATE_PAPER, "to": self.STATE_SHADOW})
        return self.state

    def transition_to_live(self) -> str:
        if self.state == self.STATE_LIVE:
            return self.state
        if self.state != self.STATE_SHADOW:
            raise CutoverError("Can only transition to LIVE from SHADOW")
        if not self.final_checks_passed:
            raise CutoverError("Final checks must pass before transitioning to LIVE")
        # perform irreversible switch
        self.state = self.STATE_LIVE
        self.capital_unlocked = True
        self._record(
            "transition",
            {
                "from": self.STATE_SHADOW,
                "to": self.STATE_LIVE,
                "capital_unlocked": True,
            },
        )
        return self.state

    def attempt_revert(self):
        # Any attempt to go back is rejected explicitly and logged
        self._record("attempt_revert", {"state": self.state})
        raise IrreversibleError("Cutover is one-way; revert is not allowed")

    def is_live(self) -> bool:
        return self.state == self.STATE_LIVE

    def get_state(self) -> str:
        return self.state

    def unlock_capital(self):
        if self.state != self.STATE_LIVE:
            raise CutoverError("Capital can only be unlocked in LIVE state")
        self.capital_unlocked = True
        self._record("unlock_capital", {"capital_unlocked": True})
        return True
