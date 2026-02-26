from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


def _ensure_duckdb():
    try:
        import duckdb  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "duckdb is required for monitoring store. Install with requirements-ops.txt"
        ) from exc
    return duckdb


def get_default_db_path(var_root: Path | None = None) -> Path:
    base = Path(var_root) if var_root is not None else Path("octa") / "var"
    return base / "metrics" / "metrics.duckdb"


def ensure_db(db_path: Path) -> None:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb = _ensure_duckdb()
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs(
                run_id TEXT PRIMARY KEY,
                ts_start TEXT,
                ts_end TEXT,
                status TEXT,
                git_sha TEXT,
                config_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS survivors(
                run_id TEXT,
                layer TEXT,
                symbol TEXT,
                timeframe TEXT,
                decision TEXT,
                reason_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metrics(
                run_id TEXT,
                layer TEXT,
                symbol TEXT,
                timeframe TEXT,
                key TEXT,
                value DOUBLE,
                ts TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events(
                run_id TEXT,
                severity TEXT,
                component TEXT,
                message TEXT,
                payload_json TEXT,
                ts TEXT
            )
            """
        )


@contextmanager
def connect(db_path: Path | None = None) -> Iterator[Any]:
    duckdb = _ensure_duckdb()
    path = Path(db_path) if db_path is not None else get_default_db_path()
    ensure_db(path)
    conn = duckdb.connect(str(path))
    try:
        yield conn
    finally:
        conn.close()
