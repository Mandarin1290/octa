from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from typing import Dict, Iterable, List, Optional

from octa_fabric.fingerprint import canonicalize

from .crypto import sign_batch, verify_batch_signature
from .events import AuditEvent


class AuditStoreError(Exception):
    pass


class LedgerStore:
    def __init__(
        self, path: str, batch_size: int = 10, signing_key_bytes: Optional[bytes] = None
    ) -> None:
        os.makedirs(path, exist_ok=True)
        self.log_path = os.path.join(path, "ledger.log")
        self.db_path = os.path.join(path, "ledger.db")
        self.batch_size = batch_size
        self.signing_key_bytes = signing_key_bytes
        self._conn = sqlite3.connect(self.db_path, isolation_level=None)
        self._init_db()

    def _init_db(self) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS events(
                event_id TEXT PRIMARY KEY,
                timestamp TEXT,
                action TEXT,
                severity TEXT,
                prev_hash TEXT,
                curr_hash TEXT,
                offset INTEGER,
                size INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signatures(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_event_id TEXT,
                last_event_id TEXT,
                signature TEXT
            )
            """
        )

    def _last_hash(self) -> Optional[str]:
        cur = self._conn.cursor()
        cur.execute("SELECT curr_hash FROM events ORDER BY rowid DESC LIMIT 1")
        r = cur.fetchone()
        return r[0] if r else None

    def append(self, event: AuditEvent) -> None:
        try:
            prev = self._last_hash()
            event.prev_hash = prev
            # compute curr_hash over canonical representation with curr_hash set to None
            canon_dict = event.to_dict()
            canon_dict["curr_hash"] = None
            canon = canonicalize(canon_dict)
            event.curr_hash = hashlib.sha256(canon.encode("utf-8")).hexdigest()
            # write to file
            data = (
                json.dumps(event.to_dict(), sort_keys=True, ensure_ascii=False) + "\n"
            )
            with open(self.log_path, "ab") as fh:
                offset = fh.tell()
                b = data.encode("utf-8")
                fh.write(b)
                size = len(b)
            # insert index record
            cur = self._conn.cursor()
            cur.execute(
                "INSERT INTO events(event_id, timestamp, action, severity, prev_hash, curr_hash, offset, size) VALUES(?,?,?,?,?,?,?,?)",
                (
                    event.event_id,
                    event.timestamp,
                    event.action,
                    event.severity,
                    event.prev_hash,
                    event.curr_hash,
                    offset,
                    size,
                ),
            )
            # maybe sign batch
            self._maybe_sign_batch()
        except Exception as e:
            raise AuditStoreError(str(e)) from e

    def _maybe_sign_batch(self) -> None:
        if not self.signing_key_bytes:
            return
        cur = self._conn.cursor()
        cur.execute(
            "SELECT event_id, curr_hash FROM events ORDER BY rowid DESC LIMIT ?",
            (self.batch_size,),
        )
        rows = cur.fetchall()
        if not rows or len(rows) < self.batch_size:
            return
        # rows are newest-first; reverse
        rows = list(reversed(rows))
        first_id = rows[0][0]
        last_id = rows[-1][0]
        hashes = [r[1] for r in rows]
        sig = sign_batch(self.signing_key_bytes, hashes)
        cur.execute(
            "INSERT INTO signatures(first_event_id, last_event_id, signature) VALUES(?,?,?)",
            (first_id, last_id, sig),
        )

    def verify_chain(self) -> bool:
        # verify hashes match recorded curr_hash and prev_hash links
        if not os.path.exists(self.log_path):
            # missing log considered unhealthy
            return False
        try:
            with open(self.log_path, "rb") as fh:
                prev = None
                for line in fh:
                    try:
                        obj = json.loads(line.decode("utf-8"))
                    except Exception:
                        return False
                    expected_curr = obj.get("curr_hash")
                    prev_hash = obj.get("prev_hash")
                    # recompute using same canonicalization rule (curr_hash = None)
                    canon_dict = dict(obj)
                    canon_dict["curr_hash"] = None
                    recomputed = hashlib.sha256(
                        canonicalize(canon_dict).encode("utf-8")
                    ).hexdigest()
                    if recomputed != expected_curr:
                        return False
                    if prev_hash != prev:
                        return False
                    prev = expected_curr
        except Exception:
            return False
        return True

    def iter_events(self) -> Iterable[Dict]:
        with open(self.log_path, "rb") as fh:
            for line in fh:
                yield json.loads(line.decode("utf-8"))

    def last_n(self, n: int) -> List[Dict]:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT event_id, offset, size FROM events ORDER BY rowid DESC LIMIT ?",
            (n,),
        )
        rows = cur.fetchall()
        res: List[Dict] = []
        if not rows:
            return res
        with open(self.log_path, "rb") as fh:
            for _eid, offset, size in rows:
                fh.seek(offset)
                data = fh.read(size).decode("utf-8")
                res.append(json.loads(data))
        return list(reversed(res))

    def by_time_range(self, start_ts: str, end_ts: str) -> List[Dict]:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT event_id, offset, size FROM events WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp ASC",
            (start_ts, end_ts),
        )
        rows = cur.fetchall()
        res: List[Dict] = []
        if not rows:
            return res
        with open(self.log_path, "rb") as fh:
            for _eid, offset, size in rows:
                fh.seek(offset)
                data = fh.read(size).decode("utf-8")
                res.append(json.loads(data))
        return res

    def by_action(self, action: str) -> List[Dict]:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT event_id, offset, size FROM events WHERE action = ? ORDER BY timestamp ASC",
            (action,),
        )
        rows = cur.fetchall()
        res: List[Dict] = []
        if not rows:
            return res
        with open(self.log_path, "rb") as fh:
            for _eid, offset, size in rows:
                fh.seek(offset)
                data = fh.read(size).decode("utf-8")
                res.append(json.loads(data))
        return res

    def verify_batch_signatures(self, public_key_bytes: bytes) -> bool:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT first_event_id, last_event_id, signature FROM signatures ORDER BY id ASC"
        )
        for first, last, sig in cur.fetchall():
            # load hashes
            cur.execute(
                "SELECT curr_hash FROM events WHERE event_id >= ? AND event_id <= ? ORDER BY rowid ASC",
                (first, last),
            )
            rows = cur.fetchall()
            hashes = [r[0] for r in rows]
            if not verify_batch_signature(public_key_bytes, hashes, sig):
                return False
        return True
