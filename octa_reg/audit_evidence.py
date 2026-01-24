import copy
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _canonical_serialize(obj: Any) -> str:
    # deterministic JSON serialization
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _compute_hash(snapshot: Dict[str, Any], control_ids: List[str]) -> str:
    payload = {"snapshot": snapshot, "controls": sorted(control_ids)}
    canon = _canonical_serialize(payload)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


@dataclass
class Evidence:
    id: str
    snapshot: Dict[str, Any]
    control_ids: List[str]
    ts: str
    hash: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "snapshot": copy.deepcopy(self.snapshot),
            "control_ids": list(self.control_ids),
            "ts": self.ts,
            "hash": self.hash,
        }


def create_evidence(snapshot: Dict[str, Any], control_ids: List[str]) -> Evidence:
    """Create an Evidence object. Hash is computed deterministically from snapshot+controls (timestamp excluded).

    Note: timestamp is metadata only; reproducibility relies on snapshot+control_ids.
    """
    snap_copy = copy.deepcopy(snapshot)
    eid = str(uuid.uuid4())
    ts = _utc_now_iso()
    h = _compute_hash(snap_copy, control_ids)
    return Evidence(
        id=eid, snapshot=snap_copy, control_ids=list(control_ids), ts=ts, hash=h
    )


def verify_evidence(e: Evidence) -> bool:
    """Verify evidence integrity by recomputing the hash from the stored snapshot and control ids."""
    expected = _compute_hash(e.snapshot, e.control_ids)
    return expected == e.hash


def export_evidence_json(e: Evidence) -> str:
    return _canonical_serialize(e.to_dict())


def load_evidence_json(s: str) -> Evidence:
    d = json.loads(s)
    return Evidence(
        id=d["id"],
        snapshot=d["snapshot"],
        control_ids=d["control_ids"],
        ts=d["ts"],
        hash=d["hash"],
    )


__all__ = [
    "Evidence",
    "create_evidence",
    "verify_evidence",
    "export_evidence_json",
    "load_evidence_json",
]
