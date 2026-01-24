from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from octa_governance.committee import GoLiveCommittee
from octa_ops.operators import OperatorRegistry, Role
from octa_sentinel.kill_switch import get_kill_switch
from octa_sentinel.live_checklist import LiveChecklist


class Mode(str, Enum):
    DEV = "DEV"
    PAPER = "PAPER"
    SHADOW = "SHADOW"
    LIVE = "LIVE"


@dataclass
class ModeEvent:
    ts: str
    from_mode: Mode
    to_mode: Mode
    actor: Optional[str]
    reason: Optional[str]


class ModeManager:
    """One-way mode transition manager with enforcement checks.

    Transitions must follow: DEV -> PAPER -> SHADOW -> LIVE. Enabling LIVE requires:
    - checklist latest passed
    - committee approved and matured
    - dual operator confirmation (both operators must be EMERGENCY)

    All audit events include the current mode via the provided `audit_fn` wrapper.
    """

    def __init__(
        self,
        audit_fn: Callable[[str, Dict[str, Any]], None],
        live_checklist: LiveChecklist,
        committee: GoLiveCommittee,
        operator_registry: OperatorRegistry,
    ):
        self._mode = Mode.DEV
        self.audit_fn: Callable[[str, Dict[str, Any]], None] = (
            audit_fn if audit_fn is not None else (lambda e, p: None)
        )
        self.checklist = live_checklist
        self.committee = committee
        self.ops = operator_registry
        self.history: List[ModeEvent] = []
        self._kill = get_kill_switch(audit_fn=audit_fn)

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _audit(self, event: str, payload: Dict[str, Any]):
        # include mode in every audit event
        payload = dict(payload)
        payload.setdefault("mode", self._mode.value)
        self.audit_fn(event, payload)

    def mode(self) -> Mode:
        return self._mode

    def to_paper(self, actor: Optional[str] = None, reason: Optional[str] = None):
        if self._mode != Mode.DEV:
            raise RuntimeError("illegal transition to PAPER")
        prev = self._mode
        self._mode = Mode.PAPER
        ev = ModeEvent(
            ts=self._now(),
            from_mode=prev,
            to_mode=self._mode,
            actor=actor,
            reason=reason,
        )
        self.history.append(ev)
        self._audit("mode_transition", ev.__dict__)

    def to_shadow(self, actor: Optional[str] = None, reason: Optional[str] = None):
        if self._mode != Mode.PAPER:
            raise RuntimeError("illegal transition to SHADOW")
        prev = self._mode
        self._mode = Mode.SHADOW
        ev = ModeEvent(
            ts=self._now(),
            from_mode=prev,
            to_mode=self._mode,
            actor=actor,
            reason=reason,
        )
        self.history.append(ev)
        self._audit("mode_transition", ev.__dict__)

    def enable_live(
        self,
        operator1: str,
        sig1: str,
        operator2: str,
        sig2: str,
        payload: Optional[str] = None,
        actor: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> bool:
        # must be in SHADOW
        if self._mode != Mode.SHADOW:
            raise RuntimeError("illegal transition to LIVE")

        # checklist must have passed
        try:
            latest = self.checklist.latest()
            if not latest.passed:
                self._audit("enable_live_blocked", {"reason": "checklist_failed"})
                return False
        except Exception:
            self._audit("enable_live_blocked", {"reason": "no_checklist"})
            return False

        # committee must have approved and matured
        if not self.committee.is_live_authorized():
            self._audit("enable_live_blocked", {"reason": "committee_not_authorized"})
            return False

        # verify dual operator signatures
        payload_ts = payload or self._now()
        canon = f"enable_live|{payload_ts}"
        if not self.ops.verify(operator1, canon, sig1):
            self._audit(
                "enable_live_blocked", {"reason": "invalid_sig1", "operator": operator1}
            )
            return False
        if not self.ops.verify(operator2, canon, sig2):
            self._audit(
                "enable_live_blocked", {"reason": "invalid_sig2", "operator": operator2}
            )
            return False

        # operators must be EMERGENCY role
        op1 = self.ops.get(operator1)
        op2 = self.ops.get(operator2)
        if (
            op1 is None
            or op2 is None
            or op1.role != Role.EMERGENCY
            or op2.role != Role.EMERGENCY
        ):
            self._audit("enable_live_blocked", {"reason": "operator_role_invalid"})
            return False

        # All good: transition to LIVE (irreversible except via kill-switch+incident)
        prev = self._mode
        self._mode = Mode.LIVE
        ev = ModeEvent(
            ts=self._now(),
            from_mode=prev,
            to_mode=self._mode,
            actor=actor,
            reason=reason,
        )
        self.history.append(ev)
        self._audit("mode_transition_live", ev.__dict__)
        return True

    def revert_live(
        self,
        incident: Dict[str, Any],
        actor: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> bool:
        # Reverting requires kill-switch triggered or locked and an incident provided
        ks_state = self._kill.get_state()
        if ks_state.name not in ("TRIGGERED", "LOCKED"):
            self._audit("revert_blocked", {"reason": "kill_switch_not_triggered"})
            return False

        if self._mode != Mode.LIVE:
            self._audit("revert_blocked", {"reason": "not_live"})
            return False

        # record incident and move to SHADOW
        prev = self._mode
        self._mode = Mode.SHADOW
        ev = ModeEvent(
            ts=self._now(),
            from_mode=prev,
            to_mode=self._mode,
            actor=actor,
            reason=reason,
        )
        self.history.append(ev)
        self._audit("mode_revert", {"event": ev.__dict__, "incident": incident})
        return True
