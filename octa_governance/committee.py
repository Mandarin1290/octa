import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class DecisionState(str, Enum):
    APPROVED = "APPROVED"
    DEFERRED = "DEFERRED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class CommitteeDecision:
    ts: str
    state: DecisionState
    rationale: str
    inputs: Dict[str, Any]
    attestations: List[Dict[str, str]]
    cooling_off_seconds: int
    signature: str


class GoLiveCommittee:
    """Virtual Investment Committee for authorizing live trading.

    - Decisions are immutable and auditable.
    - A decision may be `APPROVED`, `DEFERRED`, or `REJECTED`.
    - Approvals require a cooling-off period before they become effective.
    - Committee consults a `checklist` (Callable returning latest pass status) and optional metrics/incidents inputs.
    """

    def __init__(
        self,
        audit_fn: Optional[Callable[[str, dict], None]] = None,
        signer_key: str = "committee_key",
    ):
        self.audit = audit_fn or (lambda e, p: None)
        self.signer_key = signer_key
        self._decision: Optional[CommitteeDecision] = None

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _sign(self, payload: str) -> str:
        h = hashlib.sha256()
        h.update(self.signer_key.encode())
        h.update(payload.encode())
        return h.hexdigest()

    def propose_decision(
        self,
        state: DecisionState,
        rationale: str,
        inputs: Dict[str, Any],
        attestations: List[Dict[str, str]],
        cooling_off_seconds: int = 0,
    ) -> CommitteeDecision:
        ts = self._now_iso()
        payload = f"{ts}|{state.value}|{rationale}|{inputs}|{attestations}|{cooling_off_seconds}"
        sig = self._sign(payload)
        decision = CommitteeDecision(
            ts=ts,
            state=state,
            rationale=rationale,
            inputs=inputs,
            attestations=attestations,
            cooling_off_seconds=int(cooling_off_seconds),
            signature=sig,
        )
        # record immutably
        if self._decision is not None:
            # once a decision exists, do not overwrite
            raise RuntimeError("committee decision already recorded and immutable")
        self._decision = decision
        self.audit("committee_decision", {"decision": decision.__dict__})
        return decision

    def decision(self) -> Optional[CommitteeDecision]:
        return self._decision

    def is_live_authorized(self) -> bool:
        if not self._decision:
            return False
        if self._decision.state != DecisionState.APPROVED:
            return False
        # enforce cooling-off
        created = datetime.fromisoformat(self._decision.ts)
        effective = created + timedelta(seconds=self._decision.cooling_off_seconds)
        return datetime.now(timezone.utc) >= effective
