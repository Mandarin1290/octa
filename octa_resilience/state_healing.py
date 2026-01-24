from __future__ import annotations

import copy
import datetime
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class StateCorruptionError(Exception):
    pass


@dataclass
class PersistenceSimulator:
    # Simulate partial persistence failure by setting fail_next_write=True
    fail_next_write: bool = False
    # If partial_write is set, it will store an incomplete snapshot on failure
    partial_write: Optional[Dict[str, Any]] = None
    last_persisted: Optional[Dict[str, Any]] = None

    def write(self, data: Dict[str, Any]) -> bool:
        if self.fail_next_write:
            # simulate partial persistence
            self.last_persisted = (
                copy.deepcopy(self.partial_write)
                if self.partial_write is not None
                else {}
            )
            self.fail_next_write = False
            return False
        self.last_persisted = copy.deepcopy(data)
        return True


@dataclass
class CacheSimulator:
    cached: Optional[Dict[str, Any]] = None
    cached_hash: Optional[str] = None
    stale: bool = False

    def set(self, data: Dict[str, Any]):
        self.cached = copy.deepcopy(data)
        self.cached_hash = canonical_hash(self.cached)
        self.stale = False

    def mark_stale(self):
        self.stale = True

    def get(self) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        return (copy.deepcopy(self.cached), self.cached_hash)


@dataclass
class StateManager:
    state: Dict[str, Any]
    persistence: PersistenceSimulator = field(default_factory=PersistenceSimulator)
    cache: CacheSimulator = field(default_factory=CacheSimulator)
    audit_log: List[Dict[str, Any]] = field(default_factory=list)
    snapshot: Dict[str, Any] = field(init=False)
    snapshot_hash: str = field(init=False)

    def __post_init__(self):
        # create an initial immutable snapshot
        self.snapshot = copy.deepcopy(self.state)
        self.snapshot_hash = canonical_hash(self.snapshot)
        self.record_audit(
            "system", "snapshot_initialized", {"snapshot_hash": self.snapshot_hash}
        )

    def record_audit(self, actor: str, action: str, details: Dict[str, Any]):
        self.audit_log.append(
            {
                "ts": datetime.datetime.utcnow().isoformat() + "Z",
                "actor": actor,
                "action": action,
                "details": details,
            }
        )

    def snapshot_state(self, actor: str = "operator") -> None:
        self.snapshot = copy.deepcopy(self.state)
        self.snapshot_hash = canonical_hash(self.snapshot)
        self.record_audit(
            actor, "snapshot_saved", {"snapshot_hash": self.snapshot_hash}
        )

    def verify_state(
        self, candidate: Dict[str, Any], expected_hash: Optional[str] = None
    ) -> bool:
        h = canonical_hash(candidate)
        target = expected_hash or self.snapshot_hash
        return h == target

    def detect_corruption(self) -> bool:
        # Compare in-memory state to snapshot
        current_hash = canonical_hash(self.state)
        corrupted = current_hash != self.snapshot_hash
        if corrupted:
            self.record_audit(
                "system",
                "corruption_detected",
                {"current_hash": current_hash, "expected_hash": self.snapshot_hash},
            )
        return corrupted

    def read_from_cache(self) -> Dict[str, Any]:
        cached, ch = self.cache.get()
        if cached is None:
            self.record_audit("system", "cache_miss", {})
            return copy.deepcopy(self.snapshot)

        if self.cache.stale:
            # refuse to use stale cache
            self.record_audit(
                "system",
                "cache_stale_refused",
                {"cached_hash": ch, "snapshot_hash": self.snapshot_hash},
            )
            return copy.deepcopy(self.snapshot)

        # verify cached hash matches snapshot hash before using
        if ch != self.snapshot_hash:
            self.record_audit(
                "system",
                "cache_hash_mismatch_refused",
                {"cached_hash": ch, "snapshot_hash": self.snapshot_hash},
            )
            return copy.deepcopy(self.snapshot)

        self.record_audit("system", "cache_used", {"cached_hash": ch})
        return copy.deepcopy(cached)

    def persist_state(self, new_state: Dict[str, Any], actor: str = "operator") -> None:
        # verify new_state deterministic serialization and only persist if valid
        ok = self.persistence.write(new_state)
        if not ok:
            # detect partial persistence and rollback to snapshot
            self.record_audit(
                actor,
                "persistence_failed",
                {
                    "attempted_hash": canonical_hash(new_state),
                    "persisted_snapshot": self.persistence.last_persisted,
                },
            )
            # rollback in-memory to last good snapshot deterministically
            self.state = copy.deepcopy(self.snapshot)
            self.record_audit(
                "system",
                "rolled_back_to_snapshot",
                {"snapshot_hash": self.snapshot_hash},
            )
            raise StateCorruptionError(
                "Persistence failure; rolled back to last good snapshot"
            )

        # commit successful: update snapshot deterministically
        self.state = copy.deepcopy(new_state)
        self.snapshot_state(actor=actor)
        # update cache deterministically
        self.cache.set(self.snapshot)
        self.record_audit(
            actor, "persistence_committed", {"snapshot_hash": self.snapshot_hash}
        )

    def heal(self, actor: str = "system") -> None:
        # deterministic recovery from snapshot
        self.state = copy.deepcopy(self.snapshot)
        self.record_audit(
            actor, "healed_to_snapshot", {"snapshot_hash": self.snapshot_hash}
        )
