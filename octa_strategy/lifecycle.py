from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

from octa_strategy.state_machine import (
    LifecycleState,
    TransitionError,
    is_transition_allowed,
)


@dataclass
class TransitionRecord:
    from_state: str
    to_state: str
    timestamp: datetime
    doc: Optional[str] = None


class StrategyLifecycle:
    """Enforced lifecycle for a strategy.

    - States are append-only and auditable via `audit_fn`.
    - Transitions must be allowed by the state machine.
    - Documentation is required per state when transitioning.
    - Execution allowed only in `LIVE`.
    """

    def __init__(
        self, strategy_id: str, audit_fn: Optional[Callable[[str, Dict], None]] = None
    ):
        self.strategy_id = strategy_id
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.current_state: str = LifecycleState.IDEA
        self.history: List[TransitionRecord] = [
            TransitionRecord(
                from_state="NONE",
                to_state=self.current_state,
                timestamp=datetime.utcnow(),
                doc=None,
            )
        ]
        self.state_docs: Dict[str, str] = {}

    def _audit(self, event: str, payload: Dict):
        payload = dict(payload)
        payload.setdefault("strategy_id", self.strategy_id)
        payload.setdefault("state", self.current_state)
        self.audit_fn(event, payload)

    def transition_to(self, to_state: str, doc: Optional[str] = None) -> None:
        if to_state == self.current_state:
            return
        if not is_transition_allowed(self.current_state, to_state):
            raise TransitionError(
                f"Illegal transition {self.current_state} -> {to_state}"
            )
        # enforce mandatory documentation per state
        if not doc or not doc.strip():
            raise TransitionError(f"Documentation required to transition to {to_state}")

        # record transition
        rec = TransitionRecord(
            from_state=self.current_state,
            to_state=to_state,
            timestamp=datetime.utcnow(),
            doc=doc,
        )
        self.history.append(rec)
        # immutable past: do not modify history entries
        self.current_state = to_state
        self.state_docs[to_state] = doc
        self._audit(
            "strategy.transition",
            {
                "from": rec.from_state,
                "to": rec.to_state,
                "timestamp": rec.timestamp.isoformat(),
                "doc": doc,
            },
        )

    def time_in_state(self, state: Optional[str] = None) -> timedelta:
        state = state or self.current_state
        # find latest record where to_state == state
        for rec in reversed(self.history):
            if rec.to_state == state:
                return datetime.utcnow() - rec.timestamp
        return timedelta(0)

    def require_documentation(self, state: str) -> Optional[str]:
        return self.state_docs.get(state)

    def can_execute(self) -> bool:
        return self.current_state == LifecycleState.LIVE

    def assert_can_execute(self) -> None:
        if not self.can_execute():
            raise TransitionError(
                f"Strategy {self.strategy_id} not in LIVE state (current={self.current_state})"
            )
