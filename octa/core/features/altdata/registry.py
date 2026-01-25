from __future__ import annotations

import json
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional


@dataclass(frozen=True)
class StorePaths:
    root: Path
    db_path: Path


def _default_root() -> Path:
    return Path("octa") / "var" / "altdata"


def resolve_paths(root: Optional[str] = None) -> StorePaths:
    base = Path(root) if root else _default_root()
    return StorePaths(root=base, db_path=base / "altdata.duckdb")


def ensure_store(paths: StorePaths) -> None:
    paths.root.mkdir(parents=True, exist_ok=True)
    if _has_duckdb():
        _init_duckdb(paths.db_path)
        _ensure_columns_duckdb(paths.db_path)
    else:
        sqlite_path = paths.db_path.with_suffix(".sqlite")
        _init_sqlite(sqlite_path)
        _ensure_columns_sqlite(sqlite_path)


def _has_duckdb() -> bool:
    try:
        import duckdb  # type: ignore

        return True
    except Exception:
        return False


def _init_duckdb(path: Path) -> None:
    import duckdb  # type: ignore

    with duckdb.connect(str(path)) as conn:
        conn.execute(_schema_sql())


def _init_sqlite(path: Path) -> None:
    import sqlite3

    with sqlite3.connect(str(path)) as conn:
        conn.executescript(_schema_sql())


def _schema_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS altdata_market_features (
      run_id TEXT,
      timeframe TEXT,
      gate_layer TEXT,
      key TEXT,
      value DOUBLE,
      asof_ts TEXT
    );

    CREATE TABLE IF NOT EXISTS altdata_symbol_features (
      run_id TEXT,
      symbol TEXT,
      timeframe TEXT,
      gate_layer TEXT,
      key TEXT,
      value DOUBLE,
      asof_ts TEXT
    );

    CREATE TABLE IF NOT EXISTS altdata_provenance (
      run_id TEXT,
      source TEXT,
      asof_ts TEXT,
      fetched_at TEXT,
      hash TEXT,
      meta_json TEXT
    );
    """


def _ensure_columns_duckdb(path: Path) -> None:
    import duckdb  # type: ignore

    with duckdb.connect(str(path)) as conn:
        _add_column_duckdb(conn, "altdata_market_features", "asof_ts", "TEXT")
        _add_column_duckdb(conn, "altdata_symbol_features", "asof_ts", "TEXT")


def _add_column_duckdb(conn, table: str, column: str, col_type: str) -> None:
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    except Exception:
        return


def _ensure_columns_sqlite(path: Path) -> None:
    import sqlite3

    with sqlite3.connect(str(path)) as conn:
        _add_column_sqlite(conn, "altdata_market_features", "asof_ts", "TEXT")
        _add_column_sqlite(conn, "altdata_symbol_features", "asof_ts", "TEXT")


def _add_column_sqlite(conn, table: str, column: str, col_type: str) -> None:
    try:
        cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if column in cols:
            return
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    except Exception:
        return


class FeatureRegistry:
    def __init__(self, run_id: str, *, root: Optional[str] = None) -> None:
        self.run_id = run_id
        self.paths = resolve_paths(root)
        ensure_store(self.paths)
        self._use_duckdb = _has_duckdb()

    def get_feature_vector(
        self,
        *,
        symbol: str,
        timeframe: str,
        gate_layer: str,
        asof_ts: str | None = None,
    ) -> Dict[str, float]:
        rows = self._query_features(
            table="altdata_symbol_features",
            where="run_id = ? AND symbol = ? AND timeframe = ? AND gate_layer = ?",
            params=[self.run_id, symbol, timeframe, gate_layer],
            asof_ts=asof_ts,
        )
        return _latest_by_key(rows)

    def get_market_feature_vector(
        self,
        *,
        timeframe: str,
        gate_layer: str,
        asof_ts: str | None = None,
    ) -> Dict[str, float]:
        rows = self._query_features(
            table="altdata_market_features",
            where="run_id = ? AND timeframe = ? AND gate_layer = ?",
            params=[self.run_id, timeframe, gate_layer],
            asof_ts=asof_ts,
        )
        return _latest_by_key(rows)

    def provenance(self) -> Dict[str, Any]:
        rows = self._query_provenance([self.run_id], "run_id = ?")
        return {str(row[0]): json.loads(row[1]) for row in rows}

    def write_market_features(
        self,
        *,
        timeframe: str,
        gate_layer: str,
        features: Mapping[str, float],
        asof_ts: str | None = None,
    ) -> None:
        self._write_features(
            table="altdata_market_features",
            rows=[(self.run_id, timeframe, gate_layer, k, float(v), asof_ts) for k, v in features.items()],
        )

    def write_symbol_features(
        self,
        *,
        timeframe: str,
        gate_layer: str,
        features_by_symbol: Mapping[str, Mapping[str, float]],
        asof_ts: str | None = None,
    ) -> None:
        rows = []
        for symbol, feats in features_by_symbol.items():
            for key, val in feats.items():
                rows.append((self.run_id, symbol, timeframe, gate_layer, key, float(val), asof_ts))
        self._write_features(table="altdata_symbol_features", rows=rows)

    def write_provenance(
        self,
        *,
        source: str,
        asof: str,
        fetched_at: str,
        content_hash: str,
        meta: Mapping[str, Any],
    ) -> None:
        payload = json.dumps(meta, ensure_ascii=False, default=str)
        self._write_features(
            table="altdata_provenance",
            rows=[(self.run_id, source, asof, fetched_at, content_hash, payload)],
        )

    def _write_features(self, *, table: str, rows: Iterable[tuple]) -> None:
        rows_list = list(rows)
        if not rows_list:
            return
        placeholders = ",".join(["?"] * len(rows_list[0]))
        if self._use_duckdb:
            import duckdb  # type: ignore

            with duckdb.connect(str(self.paths.db_path)) as conn:
                conn.executemany(
                    f"INSERT INTO {table} VALUES ({placeholders})",
                    rows_list,
                )
        else:
            import sqlite3

            sqlite_path = self.paths.db_path.with_suffix(".sqlite")
            with sqlite3.connect(str(sqlite_path)) as conn:
                conn.executemany(
                    f"INSERT INTO {table} VALUES ({placeholders})",
                    rows_list,
                )
                conn.commit()

    def _query_features(self, *, table: str, params: list, where: str, asof_ts: str | None) -> list[tuple]:
        query = f"SELECT key, value, asof_ts FROM {table} WHERE {where}"
        if asof_ts:
            query += " AND asof_ts <= ?"
            params = list(params) + [asof_ts]
        query += " ORDER BY asof_ts"
        if self._use_duckdb:
            import duckdb  # type: ignore

            with duckdb.connect(str(self.paths.db_path)) as conn:
                return conn.execute(query, params).fetchall()
        import sqlite3

        sqlite_path = self.paths.db_path.with_suffix(".sqlite")
        with sqlite3.connect(str(sqlite_path)) as conn:
            cur = conn.execute(query, params)
            return cur.fetchall()

    def _query_provenance(self, params: list, where: str) -> list[tuple]:
        if self._use_duckdb:
            import duckdb  # type: ignore

            with duckdb.connect(str(self.paths.db_path)) as conn:
                return conn.execute(
                    f"SELECT source, meta_json FROM altdata_provenance WHERE {where}",
                    params,
                ).fetchall()
        import sqlite3

        sqlite_path = self.paths.db_path.with_suffix(".sqlite")
        with sqlite3.connect(str(sqlite_path)) as conn:
            cur = conn.execute(
                f"SELECT source, meta_json FROM altdata_provenance WHERE {where}",
                params,
            )
            return cur.fetchall()


def to_asof_ts(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    dt: Optional[datetime] = None
    if isinstance(ts, datetime):
        dt = ts
    elif hasattr(ts, "to_pydatetime"):
        try:
            dt = ts.to_pydatetime()
        except Exception:
            return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _latest_by_key(rows: Iterable[tuple]) -> Dict[str, float]:
    latest: Dict[str, float] = {}
    for key, value, _asof in rows:
        try:
            latest[str(key)] = float(value)
        except Exception:
            continue
    return latest
