from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import List

from octa_core.ids import generate_id
from octa_core.types import Timestamp


class AuditError(Exception):
    pass


@dataclass(frozen=True)
class Block:
    id: str
    prev_hash: str | None
    timestamp: Timestamp
    payload: dict
    hash: str


class AuditChain:
    def __init__(self) -> None:
        self._chain: List[Block] = []

    def append(self, payload: dict) -> Block:
        if not isinstance(payload, dict):
            raise AuditError("payload must be a dict")
        prev_hash = self._chain[-1].hash if self._chain else None
        id_val = str(generate_id("blk"))
        block_raw = {
            "id": id_val,
            "prev_hash": prev_hash,
            "timestamp": Timestamp.now().iso(),
            "payload": payload,
        }
        block_bytes = json.dumps(block_raw, sort_keys=True).encode("utf-8")
        h = hashlib.sha256(block_bytes).hexdigest()
        block = Block(
            id=id_val,
            prev_hash=prev_hash,
            timestamp=Timestamp.now(),
            payload=payload,
            hash=h,
        )
        self._chain.append(block)
        return block

    def verify(self) -> bool:
        prev = None
        for b in self._chain:
            raw = {
                "id": b.id,
                "prev_hash": b.prev_hash,
                "timestamp": b.timestamp.iso(),
                "payload": b.payload,
            }
            h = hashlib.sha256(
                json.dumps(raw, sort_keys=True).encode("utf-8")
            ).hexdigest()
            if h != b.hash:
                return False
            if b.prev_hash != prev:
                return False
            prev = b.hash
        return True
