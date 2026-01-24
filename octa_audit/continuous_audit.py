import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class SnapshotRecord:
    id: str
    ts: str
    name: str
    components: Dict[str, Any]
    evidence_hash: str


class ContinuousAudit:
    """Continuous audit engine keeping rolling snapshots, attestations and control logs.

    - Snapshots are canonical-hashed and stored with their evidence_hash.
    - `verify_snapshot` recomputes the hash to detect tampering.
    - All attestations and control effectiveness logs are appended with evidence hashes.
    """

    def __init__(self):
        self.snapshots: Dict[str, Dict[str, Any]] = {}
        self.attestations: List[Dict[str, Any]] = []
        self.control_logs: List[Dict[str, Any]] = []
        self.audit_log: List[Dict[str, Any]] = []

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def take_snapshot(self, name: str, components: Dict[str, Any]) -> str:
        ts = self._now_iso()
        # create canonical id from name+ts+components
        content = {"name": name, "ts": ts, "components": components}
        snap_id = canonical_hash(content)
        evidence = canonical_hash(content)
        record = {
            "id": snap_id,
            "ts": ts,
            "name": name,
            "components": components,
            "evidence_hash": evidence,
        }
        # store immutable evidence (do not recompute evidence_hash on read)
        self.snapshots[snap_id] = record
        self.audit_log.append(
            {
                "ts": ts,
                "action": "snapshot_taken",
                "id": snap_id,
                "name": name,
                "evidence_hash": evidence,
            }
        )
        return snap_id

    def get_snapshot(self, snap_id: str) -> Optional[Dict[str, Any]]:
        # Return the stored record (caller may inspect but should not modify evidence_hash)
        return self.snapshots.get(snap_id)

    def verify_snapshot(self, snap_id: str) -> bool:
        rec = self.snapshots.get(snap_id)
        if not rec:
            return False
        # recompute hash over content used at creation
        content = {
            "name": rec["name"],
            "ts": rec["ts"],
            "components": rec["components"],
        }
        return canonical_hash(content) == rec.get("evidence_hash")

    def attest_compliance(self, name: str, attestor: str, statement: str) -> str:
        ts = self._now_iso()
        rec = {"name": name, "attestor": attestor, "statement": statement, "ts": ts}
        rec["evidence_hash"] = canonical_hash(rec)
        self.attestations.append(rec)
        self.audit_log.append(
            {
                "ts": ts,
                "action": "attestation",
                "name": name,
                "attestor": attestor,
                "evidence_hash": rec["evidence_hash"],
            }
        )
        return rec["evidence_hash"]

    def record_control_effectiveness(
        self, control_id: str, status: str, notes: Optional[str] = None
    ) -> str:
        ts = self._now_iso()
        rec = {
            "control_id": control_id,
            "status": status,
            "notes": notes or "",
            "ts": ts,
        }
        rec["evidence_hash"] = canonical_hash(rec)
        self.control_logs.append(rec)
        self.audit_log.append(
            {
                "ts": ts,
                "action": "control_log",
                "control_id": control_id,
                "status": status,
                "evidence_hash": rec["evidence_hash"],
            }
        )
        return rec["evidence_hash"]

    def get_audit_log(self) -> List[Dict[str, Any]]:
        return list(self.audit_log)
