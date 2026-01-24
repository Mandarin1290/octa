from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import Optional

from . import messages


class NexusBus:
    """Durable local message bus using sqlite. Supports publish, claim, ack, nack.

    Consumers should call `claim()` to atomically claim one message, process it,
    then call `ack(message_id)` or `nack(message_id)`.
    """

    def __init__(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)
        self.path = os.path.join(path, "nexus_bus.db")
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self) -> None:
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS messages(
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                payload TEXT NOT NULL,
                ts TEXT NOT NULL,
                locked_by TEXT,
                locked_at REAL,
                delivered INTEGER DEFAULT 0,
                retries INTEGER DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_messages_undelivered ON messages(delivered, locked_at)
            """
        )
        self.conn.commit()

    def publish(self, msg: messages.BaseMessage) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO messages(id,type,payload,ts,delivered,retries) VALUES(?,?,?,?,0,0)",
            (msg.id, msg.type, msg.to_json(), msg.ts),
        )
        self.conn.commit()

    def _claim_one(self, consumer: str, timeout: float = 0.0) -> Optional[dict]:
        now = time.time()
        cur = self.conn.cursor()
        # try to atomically claim one unlocked undelivered message
        cur.execute(
            "UPDATE messages SET locked_by=?, locked_at=? WHERE id=(SELECT id FROM messages WHERE delivered=0 AND (locked_at IS NULL OR locked_at < ?) LIMIT 1) RETURNING id,type,payload,ts",
            (consumer, now, now - timeout),
        )
        row = cur.fetchone()
        if not row:
            return None
        self.conn.commit()
        return {"id": row[0], "type": row[1], "payload": row[2], "ts": row[3]}

    def claim(
        self, consumer: str, timeout: float = 30.0
    ) -> Optional[messages.BaseMessage]:
        """Claim a message for processing. Returns message or None."""
        with self._lock:
            row = self._claim_one(consumer, timeout)
        if not row:
            return None
        return messages.loads(row["payload"])

    def ack(self, message_id: str) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE messages SET delivered=1 WHERE id=?", (message_id,))
        self.conn.commit()

    def nack(self, message_id: str, backoff: float = 0.0) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE messages SET locked_by=NULL, locked_at=NULL, retries=retries+1 WHERE id=?",
            (message_id,),
        )
        self.conn.commit()

    def purge(self) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM messages")
        self.conn.commit()

    def list_undelivered(self) -> list[dict]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT id,type,payload,ts,locked_by,locked_at,retries FROM messages WHERE delivered=0 ORDER BY ts"
        )
        rows = cur.fetchall()
        return [
            dict(
                id=r[0],
                type=r[1],
                payload=r[2],
                ts=r[3],
                locked_by=r[4],
                locked_at=r[5],
                retries=r[6],
            )
            for r in rows
        ]

    def recent_by_type(self, typ: str, limit: int = 100) -> list[dict]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT id,type,payload,ts,locked_by,locked_at,retries FROM messages WHERE type=? ORDER BY ts DESC LIMIT ?",
            (typ, limit),
        )
        rows = cur.fetchall()
        return [
            dict(
                id=r[0],
                type=r[1],
                payload=r[2],
                ts=r[3],
                locked_by=r[4],
                locked_at=r[5],
                retries=r[6],
            )
            for r in rows
        ]
