from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from octa.core.governance.audit_chain import AuditChain


@dataclass(frozen=True)
class AutonomyEvent:
    ts: str
    level: str
    action: str
    mode: str
    details: dict[str, Any]


def write_autonomy_event_jsonl(path: Path, event: AutonomyEvent) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = _event_record(event)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True))
        handle.write("\n")


def write_autonomy_event(
    event: AutonomyEvent,
    *,
    audit_chain: AuditChain | None = None,
    jsonl_path: Path | None = None,
) -> None:
    record = _event_record(event)
    if audit_chain is not None:
        audit_chain.append({"event": "autonomy", **record})
        return
    if jsonl_path is not None:
        write_autonomy_event_jsonl(jsonl_path, event)
        return
    raise RuntimeError("No audit sink configured")


def make_event(action: str, mode: str, details: dict[str, Any], level: str = "INFO") -> AutonomyEvent:
    return AutonomyEvent(
        ts=datetime.utcnow().isoformat(),
        level=level,
        action=action,
        mode=mode,
        details=details,
    )


def _event_record(event: AutonomyEvent) -> dict[str, Any]:
    return {
        "ts": event.ts,
        "level": event.level,
        "action": event.action,
        "mode": event.mode,
        "details": event.details,
    }
