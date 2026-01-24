from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from octa.core.monitoring import store as monitoring_store


class RunRegistryError(RuntimeError):
    pass


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_duckdb() -> Any:
    try:
        import duckdb  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RunRegistryError(
            "duckdb is required for RunRegistry. Install with requirements-ops.txt"
        ) from exc
    return duckdb


def _coerce_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


@dataclass
class RunRecord:
    run_id: str
    ts_start: str
    ts_end: Optional[str]
    status: str
    git_sha: Optional[str]
    config_json: str


class RunRegistry:
    """DuckDB-backed registry for runs, survivors, metrics, and events."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        monitoring_store.ensure_db(self.db_path)

    def _connect(self):
        duckdb = _ensure_duckdb()
        return duckdb.connect(str(self.db_path))

    def record_run_start(
        self,
        *,
        run_id: str,
        config: Mapping[str, Any],
        git_sha: Optional[str] = None,
        status: str = "RUNNING",
    ) -> RunRecord:
        ts_start = _now_utc_iso()
        cfg_json = _coerce_json(config)
        with self._connect() as conn:
            conn.execute("DELETE FROM runs WHERE run_id = ?", [run_id])
            conn.execute(
                "INSERT INTO runs(run_id, ts_start, ts_end, status, git_sha, config_json) VALUES (?, ?, ?, ?, ?, ?)",
                [run_id, ts_start, None, status, git_sha, cfg_json],
            )
        return RunRecord(
            run_id=run_id,
            ts_start=ts_start,
            ts_end=None,
            status=status,
            git_sha=git_sha,
            config_json=cfg_json,
        )

    def record_run_end(self, *, run_id: str, status: str) -> None:
        ts_end = _now_utc_iso()
        with self._connect() as conn:
            conn.execute(
                "UPDATE runs SET ts_end = ?, status = ? WHERE run_id = ?",
                [ts_end, status, run_id],
            )

    def write_survivors(
        self,
        *,
        run_id: str,
        layer: str,
        rows: Iterable[Mapping[str, Any]],
    ) -> int:
        import pandas as pd

        df = pd.DataFrame(rows)
        if df.empty:
            return 0
        df = df.copy()
        df["run_id"] = run_id
        df["layer"] = layer
        cols = [
            "run_id",
            "layer",
            "symbol",
            "timeframe",
            "decision",
            "reason_json",
        ]
        df = df.reindex(columns=cols)
        with self._connect() as conn:
            conn.register("survivors_df", df)
            conn.execute(
                "INSERT INTO survivors SELECT run_id, layer, symbol, timeframe, decision, reason_json FROM survivors_df"
            )
        return int(len(df))

    def clear_survivors(self, *, run_id: str, layer: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM survivors WHERE run_id = ? AND layer = ?",
                [run_id, layer],
            )

    def emit_metric(
        self,
        *,
        run_id: str,
        layer: str,
        symbol: str,
        timeframe: str,
        key: str,
        value: float,
        ts: Optional[str] = None,
    ) -> None:
        ts_val = ts or _now_utc_iso()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO metrics(run_id, layer, symbol, timeframe, key, value, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [run_id, layer, symbol, timeframe, key, float(value), ts_val],
            )

    def emit_event(
        self,
        *,
        run_id: str,
        severity: str,
        component: str,
        message: str,
        payload: Optional[Mapping[str, Any]] = None,
        ts: Optional[str] = None,
    ) -> None:
        ts_val = ts or _now_utc_iso()
        payload_json = _coerce_json(payload or {})
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO events(run_id, severity, component, message, payload_json, ts) VALUES (?, ?, ?, ?, ?, ?)",
                [run_id, severity, component, message, payload_json, ts_val],
            )
