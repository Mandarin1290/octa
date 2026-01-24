import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _deterministic_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class AuditBlock:
    index: int
    prev_hash: str
    event: str
    payload: Dict[str, Any]
    timestamp: str
    block_hash: str


class AuditChain:
    """Immutable append-only audit chain with hash-based lineage.

    - `append(event, payload)` adds a new AuditBlock.
    - `verify()` checks chain integrity (hash links and block content).
    - `compute_lineage_hash(hypothesis_meta, data_snapshot)` returns a deterministic
      hash tying an alpha to the hypothesis and the exact data snapshot.
    """

    def __init__(self):
        self._blocks: List[AuditBlock] = []

    @staticmethod
    def _hash_block(
        prev_hash: str, event: str, payload: Dict[str, Any], timestamp: str
    ) -> str:
        buf = (
            prev_hash
            + "|"
            + event
            + "|"
            + timestamp
            + "|"
            + _deterministic_json(payload)
        )
        return hashlib.sha256(buf.encode("utf-8")).hexdigest()

    def append(self, event: str, payload: Dict[str, Any]) -> AuditBlock:
        prev_hash = self._blocks[-1].block_hash if self._blocks else "0" * 64
        ts = _now_iso()
        bh = self._hash_block(prev_hash, event, payload, ts)
        blk = AuditBlock(
            index=len(self._blocks),
            prev_hash=prev_hash,
            event=event,
            payload=payload,
            timestamp=ts,
            block_hash=bh,
        )
        self._blocks.append(blk)
        return blk

    def blocks(self) -> List[AuditBlock]:
        return list(self._blocks)

    def verify(self) -> bool:
        prev_hash = "0" * 64
        for blk in self._blocks:
            recomputed = self._hash_block(
                prev_hash, blk.event, blk.payload, blk.timestamp
            )
            if recomputed != blk.block_hash:
                return False
            prev_hash = blk.block_hash
        return True

    @staticmethod
    def compute_lineage_hash(
        hypothesis_meta: Dict[str, Any], data_snapshot: Dict[str, Any]
    ) -> str:
        # deterministic hash over hypothesis and data snapshot
        combined = {"hypothesis": hypothesis_meta, "data": data_snapshot}
        j = _deterministic_json(combined)
        return hashlib.sha256(j.encode("utf-8")).hexdigest()
