from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from . import store


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_event(
    run_id: str,
    severity: str,
    component: str,
    message: str,
    payload: Optional[Mapping[str, Any]] = None,
    ts: Optional[str] = None,
) -> None:
    ts_val = ts or _now_utc_iso()
    payload_json = json.dumps(payload or {}, ensure_ascii=False, default=str)
    with store.connect() as conn:
        conn.execute(
            "INSERT INTO events(run_id, severity, component, message, payload_json, ts) VALUES (?, ?, ?, ?, ?, ?)",
            [run_id, severity, component, message, payload_json, ts_val],
        )

