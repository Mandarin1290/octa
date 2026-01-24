from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from . import store


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_metric(
    run_id: str,
    layer: str,
    symbol: str,
    timeframe: str,
    key: str,
    value: float,
    ts: Optional[str] = None,
) -> None:
    ts_val = ts or _now_utc_iso()
    with store.connect() as conn:
        conn.execute(
            "INSERT INTO metrics(run_id, layer, symbol, timeframe, key, value, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [run_id, layer, symbol, timeframe, key, float(value), ts_val],
        )

