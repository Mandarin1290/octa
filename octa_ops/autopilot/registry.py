from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from octa.core.governance.immutability_guard import assert_write_allowed
from octa.core.utils.typing_safe import as_int
from .types import now_utc_iso, stable_hash


@dataclass
class RegistryPaths:
    root: Path

    @property
    def db_path(self) -> Path:
        return self.root / "registry.sqlite3"


class ArtifactRegistry:
    """SQLite registry for runs, gates, artifacts, and order idempotency."""

    def __init__(self, root: str = "artifacts", ctx: Optional[Mapping[str, Any]] = None) -> None:
        self.paths = RegistryPaths(root=Path(root))
        self._ctx: Dict[str, Any] = dict(ctx or {})
        self.paths.root.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.paths.db_path), timeout=30, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def set_context(self, ctx: Optional[Mapping[str, Any]]) -> None:
        self._ctx = dict(ctx or {})

    def _assert_mutation_allowed(self, *, operation: str, target: str, details: Optional[Mapping[str, Any]] = None) -> None:
        assert_write_allowed(
            self._ctx,
            operation=operation,
            target=target,
            details=details or {},
        )

    def _init_db(self) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS runs(
                run_id TEXT PRIMARY KEY,
                created_at TEXT,
                config_sha TEXT,
                status TEXT,
                note TEXT,
                training_data_hash TEXT,
                dependency_fingerprint TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS gates(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                symbol TEXT,
                timeframe TEXT,
                stage TEXT,
                status TEXT,
                reason TEXT,
                details_json TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS artifacts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                symbol TEXT,
                timeframe TEXT,
                artifact_kind TEXT,
                path TEXT,
                sha256 TEXT,
                size_bytes INTEGER,
                schema_version INTEGER,
                created_at TEXT,
                status TEXT,
                meta_json TEXT,
                training_data_hash TEXT,
                feature_code_hash TEXT,
                hyperparam_hash TEXT,
                dependency_fingerprint TEXT,
                reproducibility_manifest_hash TEXT,
                lifecycle_status TEXT DEFAULT 'RESEARCH'
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS metrics(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                symbol TEXT,
                timeframe TEXT,
                stage TEXT,
                metrics_json TEXT,
                gate_json TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS promotions(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timeframe TEXT,
                artifact_id INTEGER,
                level TEXT,
                created_at TEXT,
                UNIQUE(symbol, timeframe, level)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS orders(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                order_key TEXT,
                client_order_id TEXT,
                symbol TEXT,
                timeframe TEXT,
                model_id TEXT,
                side TEXT,
                qty REAL,
                status TEXT,
                created_at TEXT,
                UNIQUE(order_key)
            )
            """
        )

        # ---- forward-compatible migrations (sqlite is permissive) ----
        try:
            cur.execute("PRAGMA table_info(artifacts)")
            cols = {str(r[1]) for r in cur.fetchall()}
            if "size_bytes" not in cols:
                cur.execute("ALTER TABLE artifacts ADD COLUMN size_bytes INTEGER")
            # I1 institutional upgrade: reproducibility + lifecycle columns
            _new_artifact_cols = {
                "training_data_hash": "TEXT",
                "feature_code_hash": "TEXT",
                "hyperparam_hash": "TEXT",
                "dependency_fingerprint": "TEXT",
                "reproducibility_manifest_hash": "TEXT",
                "lifecycle_status": "TEXT DEFAULT 'RESEARCH'",
            }
            for _col, _col_type in _new_artifact_cols.items():
                if _col not in cols:
                    cur.execute(f"ALTER TABLE artifacts ADD COLUMN {_col} {_col_type}")
        except Exception:
            pass

        try:
            cur.execute("PRAGMA table_info(runs)")
            run_cols = {str(r[1]) for r in cur.fetchall()}
            # I1 institutional upgrade: reproducibility columns on runs
            _new_run_cols = {
                "training_data_hash": "TEXT",
                "dependency_fingerprint": "TEXT",
            }
            for _col, _col_type in _new_run_cols.items():
                if _col not in run_cols:
                    cur.execute(f"ALTER TABLE runs ADD COLUMN {_col} {_col_type}")
        except Exception:
            pass

    def record_run_start(self, run_id: str, config: Dict[str, Any]) -> str:
        self._assert_mutation_allowed(operation="registry_write", target="runs", details={"action": "record_run_start", "run_id": str(run_id)})
        cfg_sha = stable_hash(config)
        cur = self._conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO runs(run_id, created_at, config_sha, status, note) VALUES(?,?,?,?,?)",
            (run_id, now_utc_iso(), cfg_sha, "RUNNING", None),
        )
        return cfg_sha

    def record_run_end(self, run_id: str, status: str, note: Optional[str] = None) -> None:
        self._assert_mutation_allowed(operation="registry_write", target="runs", details={"action": "record_run_end", "run_id": str(run_id), "status": str(status)})
        cur = self._conn.cursor()
        cur.execute(
            "UPDATE runs SET status=?, note=? WHERE run_id=?",
            (str(status), note, run_id),
        )

    def upsert_gate(
        self,
        run_id: str,
        symbol: str,
        timeframe: str,
        stage: str,
        status: str,
        reason: Optional[str] = None,
        details_json: Optional[str] = None,
    ) -> None:
        self._assert_mutation_allowed(
            operation="registry_write",
            target="gates",
            details={"action": "upsert_gate", "run_id": str(run_id), "symbol": str(symbol), "timeframe": str(timeframe), "stage": str(stage)},
        )
        cur = self._conn.cursor()
        cur.execute(
            "INSERT INTO gates(run_id, symbol, timeframe, stage, status, reason, details_json, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (run_id, symbol, timeframe, stage, status, reason, details_json, now_utc_iso()),
        )

    def add_artifact(
        self,
        run_id: str,
        symbol: str,
        timeframe: str,
        artifact_kind: str,
        path: str,
        sha256: str,
        schema_version: int,
        meta_json: Optional[str] = None,
        status: str = "ACTIVE",
    ) -> int:
        self._assert_mutation_allowed(
            operation="registry_write",
            target="artifacts",
            details={"action": "add_artifact", "run_id": str(run_id), "symbol": str(symbol), "timeframe": str(timeframe), "artifact_kind": str(artifact_kind), "path": str(path)},
        )
        try:
            size_bytes = int(os.path.getsize(path)) if path and os.path.exists(path) else None
        except Exception:
            size_bytes = None
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT INTO artifacts(run_id, symbol, timeframe, artifact_kind, path, sha256, size_bytes, schema_version, created_at, status, meta_json)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                run_id,
                symbol,
                timeframe,
                artifact_kind,
                path,
                sha256,
                size_bytes,
                int(schema_version),
                now_utc_iso(),
                status,
                meta_json,
            ),
        )
        return as_int(cur.lastrowid, default=0)

    def add_metrics(
        self,
        *,
        run_id: str,
        symbol: str,
        timeframe: str,
        stage: str,
        metrics_json: Optional[str],
        gate_json: Optional[str],
    ) -> int:
        self._assert_mutation_allowed(
            operation="registry_write",
            target="metrics",
            details={"action": "add_metrics", "run_id": str(run_id), "symbol": str(symbol), "timeframe": str(timeframe), "stage": str(stage)},
        )
        cur = self._conn.cursor()
        cur.execute(
            "INSERT INTO metrics(run_id, symbol, timeframe, stage, metrics_json, gate_json, created_at) VALUES(?,?,?,?,?,?,?)",
            (
                str(run_id),
                str(symbol),
                str(timeframe),
                str(stage),
                metrics_json,
                gate_json,
                now_utc_iso(),
            ),
        )
        return as_int(cur.lastrowid, default=0)

    def set_artifact_status(self, artifact_id: int, status: str, note_json: Optional[str] = None) -> None:
        self._assert_mutation_allowed(
            operation="registry_write",
            target="artifacts",
            details={"action": "set_artifact_status", "artifact_id": int(artifact_id), "status": str(status)},
        )
        cur = self._conn.cursor()
        if note_json is not None:
            cur.execute("UPDATE artifacts SET status=?, meta_json=? WHERE id=?", (str(status), str(note_json), int(artifact_id)))
        else:
            cur.execute("UPDATE artifacts SET status=? WHERE id=?", (str(status), int(artifact_id)))

    def promote(self, symbol: str, timeframe: str, artifact_id: int, level: str) -> None:
        self._assert_mutation_allowed(
            operation="registry_write",
            target="promotions",
            details={"action": "promote", "symbol": str(symbol), "timeframe": str(timeframe), "artifact_id": int(artifact_id), "level": str(level)},
        )
        cur = self._conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO promotions(symbol, timeframe, artifact_id, level, created_at) VALUES(?,?,?,?,?)",
            (symbol, timeframe, int(artifact_id), str(level), now_utc_iso()),
        )

    def clear_promotion(self, *, symbol: str, timeframe: str, level: str) -> None:
        """Remove a promotion mapping (fail-closed demotion).

        This prevents paper/live runners from using stale artifacts when a new run FAILs/SKIPs.
        """
        self._assert_mutation_allowed(
            operation="registry_write",
            target="promotions",
            details={"action": "clear_promotion", "symbol": str(symbol), "timeframe": str(timeframe), "level": str(level)},
        )
        cur = self._conn.cursor()
        cur.execute(
            "DELETE FROM promotions WHERE symbol=? AND timeframe=? AND level=?",
            (str(symbol), str(timeframe), str(level)),
        )

    def get_promoted_artifacts(self, level: str = "paper") -> List[Dict[str, Any]]:
        cur = self._conn.cursor()
        if str(level) == "live":
            # Fail-closed for live: LIVE status only. No NULL backward-compat.
            cur.execute(
                """
                SELECT p.symbol, p.timeframe, a.id as artifact_id, a.path, a.sha256, a.schema_version, a.meta_json
                FROM promotions p
                JOIN artifacts a ON a.id = p.artifact_id
                WHERE p.level = ? AND a.lifecycle_status = 'LIVE'
                ORDER BY p.symbol ASC, p.timeframe ASC
                """,
                (str(level),),
            )
        else:
            # level="paper": PAPER and LIVE are valid; NULL = pre-I1 backward compat.
            cur.execute(
                """
                SELECT p.symbol, p.timeframe, a.id as artifact_id, a.path, a.sha256, a.schema_version, a.meta_json
                FROM promotions p
                JOIN artifacts a ON a.id = p.artifact_id
                WHERE p.level = ?
                  AND (a.lifecycle_status IN ('PAPER', 'LIVE') OR a.lifecycle_status IS NULL)
                ORDER BY p.symbol ASC, p.timeframe ASC
                """,
                (str(level),),
            )
        return [dict(r) for r in cur.fetchall()]

    def get_lifecycle_status(self, artifact_id: int) -> Optional[str]:
        """Read lifecycle_status of an artifact. Returns None for missing or pre-I1 NULL rows."""
        cur = self._conn.cursor()
        cur.execute("SELECT lifecycle_status FROM artifacts WHERE id=?", (int(artifact_id),))
        row = cur.fetchone()
        if row is None:
            return None
        return row[0]  # may be None for pre-I1 rows

    def try_reserve_order(self, *, run_id: str, order_key: str, client_order_id: str, symbol: str, timeframe: str, model_id: str, side: str, qty: float) -> bool:
        self._assert_mutation_allowed(
            operation="registry_write",
            target="orders",
            details={"action": "try_reserve_order", "run_id": str(run_id), "symbol": str(symbol), "timeframe": str(timeframe), "order_key": str(order_key)},
        )
        cur = self._conn.cursor()
        try:
            cur.execute(
                "INSERT INTO orders(run_id, order_key, client_order_id, symbol, timeframe, model_id, side, qty, status, created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (run_id, order_key, client_order_id, symbol, timeframe, model_id, side, float(qty), "RESERVED", now_utc_iso()),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def set_order_status(self, order_key: str, status: str) -> None:
        self._assert_mutation_allowed(
            operation="registry_write",
            target="orders",
            details={"action": "set_order_status", "order_key": str(order_key), "status": str(status)},
        )
        cur = self._conn.cursor()
        cur.execute("UPDATE orders SET status=? WHERE order_key=?", (str(status), str(order_key)))

    def set_lifecycle_status(self, artifact_id: int, lifecycle_status: str) -> None:
        """Update the lifecycle_status of an artifact (I1: RESEARCH→SHADOW→PAPER→LIVE→RETIRED|QUARANTINED)."""
        self._assert_mutation_allowed(
            operation="registry_write",
            target="artifacts",
            details={"action": "set_lifecycle_status", "artifact_id": int(artifact_id), "lifecycle_status": str(lifecycle_status)},
        )
        cur = self._conn.cursor()
        cur.execute(
            "UPDATE artifacts SET lifecycle_status=? WHERE id=?",
            (str(lifecycle_status), int(artifact_id)),
        )
