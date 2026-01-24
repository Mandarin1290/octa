from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from .hashing import stable_hash


@dataclass(frozen=True)
class AuditRecord:
    index: int
    ts: str
    prev_hash: str
    payload: Mapping[str, Any]
    hash: str


def _record_payload(index: int, ts: str, prev_hash: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "index": index,
        "ts": ts,
        "prev_hash": prev_hash,
        "payload": payload,
    }


class AuditChain:
    def __init__(self, ledger_path: Path) -> None:
        self._path = ledger_path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, payload: Mapping[str, Any], ts: datetime | None = None) -> AuditRecord:
        current_ts = (ts or datetime.utcnow()).isoformat()
        index, prev_hash = self._last_index_hash()
        record_payload = _record_payload(index + 1, current_ts, prev_hash, payload)
        record_hash = stable_hash(record_payload)
        record = AuditRecord(
            index=index + 1,
            ts=current_ts,
            prev_hash=prev_hash,
            payload=payload,
            hash=record_hash,
        )
        with self._path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "index": record.index,
                        "ts": record.ts,
                        "prev_hash": record.prev_hash,
                        "payload": record.payload,
                        "hash": record.hash,
                    },
                    sort_keys=True,
                )
            )
            handle.write("\n")
        return record

    def verify(self) -> bool:
        if not self._path.exists():
            return True
        prev_hash = "GENESIS"
        index = 0
        with self._path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                expected_payload = _record_payload(data["index"], data["ts"], data["prev_hash"], data["payload"])
                expected_hash = stable_hash(expected_payload)
                if data["prev_hash"] != prev_hash:
                    return False
                if data["index"] != index + 1:
                    return False
                if data["hash"] != expected_hash:
                    return False
                prev_hash = data["hash"]
                index = data["index"]
        return True

    def _last_index_hash(self) -> tuple[int, str]:
        if not self._path.exists():
            return 0, "GENESIS"
        last_line = ""
        with self._path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    last_line = line
        if not last_line:
            return 0, "GENESIS"
        data = json.loads(last_line)
        return int(data.get("index", 0)), str(data.get("hash", "GENESIS"))
