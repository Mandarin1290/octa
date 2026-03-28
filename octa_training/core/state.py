from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


class StateError(Exception):
    pass


class StateRegistry:
    """Persistent state registry backed by SQLite.

    Ensures atomic writes and simple API for symbol states.
    """

    def __init__(self, state_dir: Path):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.state_dir / "state.db"
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self) -> None:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_state (
                    symbol TEXT PRIMARY KEY,
                    last_seen_parquet_hash TEXT,
                    last_parquet_mtime REAL,
                    last_parquet_size INTEGER,
                    asset_class TEXT,
                    asset_config_overlay_path TEXT,
                    last_device_profile TEXT,
                    last_train_time TEXT,
                    last_pass_time TEXT,
                    last_fail_time TEXT,
                    fail_count INTEGER DEFAULT 0,
                    last_metrics_summary TEXT,
                    last_gate_result TEXT,
                    artifact_path TEXT,
                    artifact_hash TEXT,
                    artifact_smoke_test_status TEXT,
                    artifact_smoke_test_time TEXT,
                    artifact_quarantine_path TEXT,
                    artifact_quarantine_reason TEXT
                )
                """
            )
            # persistent queue for daemon
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS run_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    reason TEXT,
                    priority INTEGER DEFAULT 50,
                    not_before_ts REAL,
                    enqueued_at TEXT,
                    status TEXT DEFAULT 'PENDING',
                    run_id TEXT
                )
                """
            )
            # running jobs table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS running_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    run_id TEXT,
                    started_at TEXT,
                    status TEXT
                )
                """
            )
            conn.commit()

            # try to add new columns if they don't exist (safe add)
            try:
                cur.execute("ALTER TABLE symbol_state ADD COLUMN artifact_quarantine_path TEXT")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE symbol_state ADD COLUMN artifact_quarantine_reason TEXT")
            except Exception:
                pass
            # add delisting fields if not present
            try:
                cur.execute("ALTER TABLE symbol_state ADD COLUMN delisted INTEGER DEFAULT 0")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE symbol_state ADD COLUMN delisting_date TEXT")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE symbol_state ADD COLUMN delisted_reason TEXT")
            except Exception:
                pass

            # asset overlay observability
            try:
                cur.execute("ALTER TABLE symbol_state ADD COLUMN asset_config_overlay_path TEXT")
            except Exception:
                pass
            # gate-aware idempotence: track which gate config produced the last pass
            try:
                cur.execute("ALTER TABLE symbol_state ADD COLUMN last_gate_config_id TEXT")
            except Exception:
                pass

    def _connect(self):
        conn = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None)
        # enable WAL for better concurrency and atomicity
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.row_factory = sqlite3.Row
        return conn

    def get_symbol_state(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM symbol_state WHERE symbol = ?", (symbol,))
            row = cur.fetchone()
            if not row:
                return None
            data = dict(row)
            # parse JSON field
            if data.get("last_metrics_summary"):
                try:
                    data["last_metrics_summary"] = json.loads(data["last_metrics_summary"])
                except Exception:
                    data["last_metrics_summary"] = None
            return data
    # Queue operations
    def enqueue(self, symbol: str, reason: str = "file_change", priority: int = 50, not_before_ts: Optional[float] = None, run_id: Optional[str] = None) -> int:
        now = datetime.utcnow().isoformat()
        with self._lock, self._connect() as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO run_queue(symbol,reason,priority,not_before_ts,enqueued_at,status,run_id) VALUES (?,?,?,?,?,?,?)", (symbol, reason, priority, not_before_ts, now, 'PENDING', run_id))
            conn.commit()
            return cur.lastrowid

    def dequeue_ready(self) -> Optional[Dict[str, Any]]:
        from time import time
        now_ts = time()
        with self._lock, self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id,symbol,reason,priority,not_before_ts,enqueued_at,run_id FROM run_queue WHERE status='PENDING' AND (not_before_ts IS NULL OR not_before_ts <= ?) ORDER BY priority ASC, enqueued_at ASC LIMIT 1", (now_ts,))
            row = cur.fetchone()
            if not row:
                return None
            data = dict(row)
            cur.execute("UPDATE run_queue SET status='LOCKED' WHERE id = ?", (data['id'],))
            conn.commit()
            return data

    def mark_running(self, qid: int, symbol: str, run_id: str) -> None:
        now = datetime.utcnow().isoformat()
        with self._lock, self._connect() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE run_queue SET status='RUNNING', run_id=? WHERE id = ?", (run_id, qid))
            cur.execute("INSERT INTO running_jobs(symbol,run_id,started_at,status) VALUES (?,?,?,?)", (symbol, run_id, now, 'RUNNING'))
            conn.commit()

    def mark_done(self, qid: int, run_id: str, status: str = 'SUCCESS') -> None:
        with self._lock, self._connect() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE run_queue SET status=? WHERE id = ?", (status, qid))
            cur.execute("UPDATE running_jobs SET status=? WHERE run_id = ?", (status, run_id))
            conn.commit()

    def list_queue(self, status: Optional[str] = None) -> list:
        with self._lock, self._connect() as conn:
            cur = conn.cursor()
            if status:
                cur.execute("SELECT * FROM run_queue WHERE status = ? ORDER BY priority, enqueued_at", (status,))
            else:
                cur.execute("SELECT * FROM run_queue ORDER BY priority, enqueued_at")
            return [dict(r) for r in cur.fetchall()]

    def abort_stale_running(self, timeout_seconds: int = 3600) -> list:
        # mark running jobs older than timeout as ABORTED and re-enqueue
        requeued = []
        with self._lock, self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id,symbol,run_id,started_at FROM running_jobs WHERE status='RUNNING'")
            rows = cur.fetchall()
            for r in rows:
                started = r['started_at']
                try:
                    started_dt = datetime.fromisoformat(started)
                except Exception:
                    continue
                age = (datetime.utcnow() - started_dt).total_seconds()
                if age > timeout_seconds:
                    cur.execute("UPDATE running_jobs SET status='ABORTED' WHERE id = ?", (r['id'],))
                    # re-enqueue
                    self.enqueue(r['symbol'], reason='stale_abort', priority=60)
                    requeued.append(r['symbol'])
            conn.commit()
        return requeued

    def list_symbols(self) -> List[str]:
        with self._lock, self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT symbol FROM symbol_state ORDER BY symbol")
            return [r[0] for r in cur.fetchall()]

    def update_symbol_state(self, symbol: str, **kwargs) -> None:
        """Upsert symbol state; kwargs can include any column fields.

        last_metrics_summary will be JSON-serialized if present.
        """
        allowed = {
            "last_seen_parquet_hash",
            "last_parquet_mtime",
            "last_parquet_size",
            "asset_class",
            "asset_config_overlay_path",
            "last_device_profile",
            "last_train_time",
            "last_pass_time",
            "last_fail_time",
            "fail_count",
            "last_metrics_summary",
            "last_gate_result",
            "artifact_path",
            "artifact_hash",
            "artifact_smoke_test_status",
            "artifact_smoke_test_time",
            "artifact_quarantine_path",
            "artifact_quarantine_reason",
            "delisted",
            "delisting_date",
            "delisted_reason",
            # gate-aware idempotence: policy version that produced the last pass
            "last_gate_config_id",
        }
        to_write = {k: v for k, v in kwargs.items() if k in allowed}
        if "last_metrics_summary" in to_write and to_write["last_metrics_summary"] is not None:
            to_write["last_metrics_summary"] = json.dumps(to_write["last_metrics_summary"], ensure_ascii=False)

        with self._lock, self._connect() as conn:
            cur = conn.cursor()
            # check exists
            cur.execute("SELECT 1 FROM symbol_state WHERE symbol = ?", (symbol,))
            exists = cur.fetchone()
            if not exists:
                # insert default row
                cur.execute(
                    "INSERT INTO symbol_state(symbol) VALUES (?)",
                    (symbol,)
                )
            # perform update
            for k, v in to_write.items():
                cur.execute(f"UPDATE symbol_state SET {k} = ? WHERE symbol = ?", (v, symbol))
            conn.commit()

    def record_run_start(self, symbol: str, run_id: str) -> None:
        now = datetime.utcnow().isoformat()
        self.update_symbol_state(symbol, last_train_time=now)

    def record_run_end(self, symbol: str, run_id: str, passed: bool, metrics_summary: Optional[Dict[str, Any]] = None) -> None:
        now = datetime.utcnow().isoformat()
        if passed:
            self.update_symbol_state(symbol, last_pass_time=now, last_gate_result="PASS", fail_count=0, last_metrics_summary=metrics_summary)
        else:
            # increment fail_count
            cur_state = self.get_symbol_state(symbol) or {}
            fc = int(cur_state.get("fail_count") or 0) + 1
            self.update_symbol_state(symbol, last_fail_time=now, fail_count=fc, last_gate_result="FAIL", last_metrics_summary=metrics_summary)
