from __future__ import annotations

import hashlib
import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_json(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str).encode("utf-8")


@dataclass(frozen=True)
class AuditEntry:
    ts: str
    event_type: str
    payload: Dict[str, Any]
    prev_hash: str
    hash: str


class AuditLog:
    """Append-only JSONL audit log with hash chaining.

    This is independent of octa_ledger. Use it for control-plane and security events.
    """

    def __init__(self, *, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _read_last_hash(self) -> str:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return "0" * 64
        try:
            # Read the last non-empty line efficiently.
            with self.path.open("rb") as fh:
                fh.seek(0, os.SEEK_END)
                end = fh.tell()
                if end == 0:
                    return "0" * 64

                # Skip trailing newlines/spaces.
                i = end - 1
                while i >= 0:
                    fh.seek(i)
                    b = fh.read(1)
                    if b not in {b"\n", b"\r", b" ", b"\t"}:
                        break
                    i -= 1
                if i < 0:
                    return "0" * 64

                # Find the start of the last line.
                while i >= 0:
                    fh.seek(i)
                    if fh.read(1) == b"\n":
                        i += 1
                        break
                    i -= 1
                if i < 0:
                    i = 0
                fh.seek(i)
                line = fh.readline().decode("utf-8").strip()
            if not line:
                return "0" * 64
            obj = json.loads(line)
            return str(obj.get("hash") or ("0" * 64))
        except Exception as e:
            # Fail-closed: if we cannot parse, treat as corrupted.
            raise RuntimeError("audit_log_corrupt_or_unreadable") from e

    def append(self, *, event_type: str, payload: Dict[str, Any]) -> AuditEntry:
        with self._lock:
            prev_hash = self._read_last_hash()
            base = {
                "ts": _now_iso(),
                "event_type": str(event_type),
                "payload": payload or {},
                "prev_hash": prev_hash,
            }
            h = _sha256_hex(_canonical_json(base))
            rec = {**base, "hash": h}
            line = json.dumps(rec, ensure_ascii=False, sort_keys=True, default=str)
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
            return AuditEntry(ts=base["ts"], event_type=base["event_type"], payload=base["payload"], prev_hash=prev_hash, hash=h)

    def verify(self) -> Tuple[bool, Optional[str]]:
        """Verify hash chain. Returns (ok, error_reason)."""
        if not self.path.exists():
            return True, None

        prev = "0" * 64
        try:
            with self.path.open("r", encoding="utf-8") as fh:
                for i, line in enumerate(fh, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    if str(obj.get("prev_hash")) != prev:
                        return False, f"prev_hash_mismatch_line_{i}"
                    base = {
                        "ts": obj.get("ts"),
                        "event_type": obj.get("event_type"),
                        "payload": obj.get("payload") or {},
                        "prev_hash": obj.get("prev_hash"),
                    }
                    expected = _sha256_hex(_canonical_json(base))
                    if str(obj.get("hash")) != expected:
                        return False, f"hash_mismatch_line_{i}"
                    prev = expected
        except Exception as e:
            return False, f"verify_exception:{e}"

        return True, None


__all__ = ["AuditLog", "AuditEntry"]
