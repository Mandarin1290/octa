import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:  # type: ignore
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class SunsetRecord:
    ts: str
    strategy: str
    state: str
    reason: Optional[str]
    reclaimed: Optional[float]
    evidence_hash: str


class SunsetEngine:
    """Engine to retire strategies safely and systematically.

    States:
    - `active`
    - `sunset` (shutdown in progress)
    - `retired` (final; reversible only via governance)

    Triggers accepted programmatically: `alpha_decay`, `capacity_breach`, `regime_mismatch`.
    All actions are logged in `audit_log` with canonical evidence hashes.
    """

    def __init__(self):
        # strategy -> dict(state, capital)
        self._strategies: Dict[str, Dict[str, Any]] = {}
        self.audit_log: List[Dict[str, Any]] = []
        self.history: List[SunsetRecord] = []

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _record_audit(self, action: str, details: Dict[str, Any]):
        ts = self._now_iso()
        rec = {"ts": ts, "action": action, "details": details}
        rec["evidence_hash"] = canonical_hash(rec)
        self.audit_log.append(rec)
        return rec["evidence_hash"]

    def add_strategy(self, name: str, capital: float):
        self._strategies[name] = {"state": "active", "capital": float(capital)}
        h = self._record_audit(
            "add_strategy", {"strategy": name, "capital": float(capital)}
        )
        self.history.append(
            SunsetRecord(
                ts=self._now_iso(),
                strategy=name,
                state="active",
                reason=None,
                reclaimed=None,
                evidence_hash=h,
            )
        )

    def get_state(self, name: str) -> Optional[str]:
        s = self._strategies.get(name)
        return s.get("state") if s else None

    def get_capital(self, name: str) -> Optional[float]:
        s = self._strategies.get(name)
        return s.get("capital") if s else None

    def initiate_sunset(
        self, name: str, trigger: str, notes: Optional[str] = None
    ) -> str:
        s = self._strategies.get(name)
        if s is None:
            raise KeyError("unknown strategy")
        if s["state"] == "retired":
            raise RuntimeError("strategy already retired; reversal requires governance")
        s["state"] = "sunset"
        evidence = self._record_audit(
            "sunset_initiated", {"strategy": name, "trigger": trigger, "notes": notes}
        )
        self.history.append(
            SunsetRecord(
                ts=self._now_iso(),
                strategy=name,
                state="sunset",
                reason=trigger,
                reclaimed=None,
                evidence_hash=evidence,
            )
        )
        return evidence

    def perform_shutdown(self, name: str) -> float:
        """Orderly shutdown: stop new allocations and reclaim capital.

        Returns reclaimed capital amount.
        """
        s = self._strategies.get(name)
        if s is None:
            raise KeyError("unknown strategy")
        if s["state"] not in ("sunset", "active"):
            raise RuntimeError("shutdown not allowed in current state")

        # Simulate reclamation: assume we can reclaim full capital for orderly shutdown
        reclaimed = float(s.get("capital", 0.0))
        s["capital"] = 0.0
        s["state"] = "retired"
        evidence = self._record_audit(
            "shutdown_complete", {"strategy": name, "reclaimed": reclaimed}
        )
        self.history.append(
            SunsetRecord(
                ts=self._now_iso(),
                strategy=name,
                state="retired",
                reason="shutdown",
                reclaimed=reclaimed,
                evidence_hash=evidence,
            )
        )
        return reclaimed

    def reinstate(
        self,
        name: str,
        governance_approval: bool = False,
        approver: Optional[str] = None,
    ) -> str:
        """Reinstate a retired strategy only with governance approval.

        Returns evidence hash. Raises PermissionError without governance_approval.
        """
        s = self._strategies.get(name)
        if s is None:
            raise KeyError("unknown strategy")
        if s["state"] != "retired":
            raise RuntimeError("only retired strategies may be reinstated")
        if not governance_approval:
            raise PermissionError("reinstatement requires governance approval")
        s["state"] = "active"
        evidence = self._record_audit(
            "reinstated_via_governance", {"strategy": name, "approver": approver}
        )
        self.history.append(
            SunsetRecord(
                ts=self._now_iso(),
                strategy=name,
                state="active",
                reason="reinstated",
                reclaimed=None,
                evidence_hash=evidence,
            )
        )
        return evidence

    def get_audit(self) -> List[Dict[str, Any]]:
        return list(self.audit_log)


from dataclasses import dataclass
