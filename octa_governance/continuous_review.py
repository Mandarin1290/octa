import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:  # type: ignore
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class ReviewRecord:
    ts: str
    cycle: str
    participants: Optional[List[str]]
    notes: Optional[str]
    evidence_hash: str


class ContinuousReviewLoop:
    """Continuous governance review loop.

    - Supports daily risk review, weekly strategy review, monthly governance committee.
    - All reviews are appended to `audit_log` with a canonical evidence hash.
    - Provides explicit trigger methods and a scheduler check `run_scheduled(now)`.
    """

    def __init__(self):
        self.audit_log: List[Dict[str, Any]] = []
        # last run timestamps per cycle name (ISO strings)
        self.last_run: Dict[str, str] = {}

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _record(
        self,
        cycle: str,
        participants: Optional[List[str]] = None,
        notes: Optional[str] = None,
    ) -> ReviewRecord:
        ts = self._now_iso()
        rec = {
            "ts": ts,
            "cycle": cycle,
            "participants": participants or [],
            "notes": notes,
        }
        evidence: str = canonical_hash(rec)
        rec["evidence_hash"] = evidence
        self.audit_log.append(rec)
        self.last_run[cycle] = ts
        return ReviewRecord(
            ts=ts,
            cycle=cycle,
            participants=participants,
            notes=notes,
            evidence_hash=evidence,
        )

    def trigger_daily_risk_review(
        self, participants: Optional[List[str]] = None, notes: Optional[str] = None
    ) -> ReviewRecord:
        return self._record("daily_risk", participants, notes)

    def trigger_weekly_strategy_review(
        self, participants: Optional[List[str]] = None, notes: Optional[str] = None
    ) -> ReviewRecord:
        return self._record("weekly_strategy", participants, notes)

    def trigger_monthly_committee(
        self, participants: Optional[List[str]] = None, notes: Optional[str] = None
    ) -> ReviewRecord:
        return self._record("monthly_committee", participants, notes)

    def run_scheduled(self, now_iso: Optional[str] = None) -> List[ReviewRecord]:
        """Run any scheduled reviews that are due based on `last_run`.

        `now_iso` may be supplied for deterministic testing.
        """
        now = datetime.fromisoformat(now_iso) if now_iso else datetime.now(timezone.utc)
        out: List[ReviewRecord] = []

        def due(cycle: str, delta: timedelta) -> bool:
            last = self.last_run.get(cycle)
            if last is None:
                return True
            last_dt = datetime.fromisoformat(last)
            return (now - last_dt) >= delta

        if due("daily_risk", timedelta(days=1)):
            out.append(self.trigger_daily_risk_review())
        if due("weekly_strategy", timedelta(days=7)):
            out.append(self.trigger_weekly_strategy_review())
        if due("monthly_committee", timedelta(days=28)):
            out.append(self.trigger_monthly_committee())

        return out

    def get_audit(self) -> List[Dict[str, Any]]:
        return list(self.audit_log)
