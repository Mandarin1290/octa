import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional


def _utc_now(fn: Optional[Callable[[], datetime]] = None) -> datetime:
    if fn:
        return fn()
    return datetime.utcnow()


@dataclass
class Record:
    id: str
    classification: str
    payload: Dict[str, Any]
    created_ts: datetime
    retention_period: timedelta
    deleted: bool = False
    deleted_ts: Optional[datetime] = None
    deletion_justification: Optional[str] = None


class RetentionManager:
    """Manage records, enforce retention periods and secure deletion workflows.

    Hard rules:
    - Records are immutable until expiry of their retention_period.
    - Deletion before expiry is blocked.
    - All deletions are logged with justification.
    """

    def __init__(self, now_fn: Optional[Callable[[], datetime]] = None):
        self.now_fn = now_fn
        self._store: Dict[str, Record] = {}
        self.audit_log: List[Dict[str, Any]] = []

    def _now(self) -> datetime:
        return _utc_now(self.now_fn)

    def _log(
        self, action: str, details: Dict[str, Any], actor: Optional[str] = None
    ) -> None:
        self.audit_log.append(
            {
                "ts": self._now().isoformat() + "Z",
                "actor": actor,
                "action": action,
                "details": details,
            }
        )

    def create_record(
        self,
        classification: str,
        payload: Dict[str, Any],
        retention_days: int = 365,
        actor: Optional[str] = None,
    ) -> Record:
        rid = str(uuid.uuid4())
        rec = Record(
            id=rid,
            classification=classification,
            payload=dict(payload),
            created_ts=self._now(),
            retention_period=timedelta(days=retention_days),
        )
        self._store[rid] = rec
        self._log(
            "record_created",
            {
                "id": rid,
                "classification": classification,
                "retention_days": retention_days,
            },
            actor,
        )
        return rec

    def get_record(self, record_id: str) -> Record:
        return self._store[record_id]

    def is_expired(self, record: Record) -> bool:
        return (self._now() - record.created_ts) >= record.retention_period

    def attempt_delete(
        self, record_id: str, justification: str, actor: Optional[str] = None
    ) -> None:
        """Attempt secure deletion; blocks if retention not expired. Deletion logged and marked.

        Permanent purge may be implemented by `purge_expired()` which removes records that are marked deleted and expired.
        """
        if record_id not in self._store:
            raise KeyError("unknown record")
        rec = self._store[record_id]
        if rec.deleted:
            raise RuntimeError("record already deleted")

        if not self.is_expired(rec):
            # deletion before expiry is blocked and logged
            self._log(
                "deletion_blocked_premature",
                {
                    "id": record_id,
                    "expires_at": (rec.created_ts + rec.retention_period).isoformat()
                    + "Z",
                },
                actor,
            )
            raise RuntimeError("record retention period not expired")

        # mark deleted (secure deletion workflow)
        rec.deleted = True
        rec.deleted_ts = self._now()
        rec.deletion_justification = justification
        self._log(
            "record_deleted", {"id": record_id, "justification": justification}, actor
        )

    def purge_expired(self) -> List[str]:
        """Permanently remove records that are both expired and marked deleted. Returns list of purged ids."""
        to_purge = [
            rid for rid, r in self._store.items() if r.deleted and self.is_expired(r)
        ]
        for rid in to_purge:
            del self._store[rid]
            self._log("record_purged", {"id": rid}, None)
        return to_purge

    def list_records(self) -> List[Record]:
        # deterministic ordering by created_ts then id
        return sorted(
            self._store.values(), key=lambda r: (r.created_ts.isoformat(), r.id)
        )

    def classify_record(
        self, record_id: str, new_classification: str, actor: Optional[str] = None
    ) -> None:
        rec = self.get_record(record_id)
        if not self.is_expired(rec):
            raise RuntimeError("cannot modify classification before retention expiry")
        rec.classification = new_classification
        self._log(
            "record_reclassified",
            {"id": record_id, "classification": new_classification},
            actor,
        )
