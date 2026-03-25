from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass
class ScheduledEventsSource:
    cfg: Mapping[str, Any]
    name: str = "scheduled_events"

    def __post_init__(self) -> None:
        self.enabled = bool(self.cfg.get("enabled", False))
        self.window_days = int(self.cfg.get("window_days", 3) or 3)

    def cache_key(self, *, asof: date) -> str:
        return f"{self.name}_{asof.isoformat()}"

    def fetch_raw(self, *, asof: date, allow_net: bool) -> Mapping[str, Any] | None:
        if not allow_net:
            return None

        asof_start = datetime.combine(asof, datetime.min.time(), tzinfo=timezone.utc)
        window_start = asof_start - timedelta(days=1)
        window_end = asof_start + timedelta(days=max(self.window_days, 1))

        rows: list[dict[str, Any]] = []
        for raw_event in self.cfg.get("events", []) if isinstance(self.cfg, dict) else []:
            if not isinstance(raw_event, dict):
                continue
            scheduled_at = _parse_dt(raw_event.get("scheduled_at"))
            if scheduled_at is None:
                continue
            if scheduled_at < window_start or scheduled_at > window_end:
                continue
            row = {
                "event_id": str(raw_event.get("event_id", "")).strip() or f"scheduled_{len(rows)+1}",
                "title": str(raw_event.get("title", "")).strip(),
                "scheduled_at": scheduled_at.isoformat(),
                "source_id": str(raw_event.get("source_id", "scheduled")).strip(),
                "source_name": str(raw_event.get("source_name", "Scheduled Institutional Event")).strip(),
                "source_tier": int(raw_event.get("source_tier", 1)),
                "event_type": str(raw_event.get("event_type", "central_bank")).strip().lower(),
                "severity_floor": str(raw_event.get("severity_floor", "high")).strip().lower(),
                "category": str(raw_event.get("category", "scheduled_macro")).strip().lower(),
                "jurisdiction": str(raw_event.get("jurisdiction", "global")).strip(),
                "asset_classes": list(raw_event.get("asset_classes", ["all"])),
                "pre_window_hours": float(raw_event.get("pre_window_hours", 24.0) or 24.0),
                "post_window_hours": float(raw_event.get("post_window_hours", 2.0) or 2.0),
                "official": bool(raw_event.get("official", True)),
                "known_at": str(raw_event.get("known_at", "")).strip() or scheduled_at.isoformat(),
            }
            rows.append(row)

        rows.sort(key=lambda row: (row["scheduled_at"], row["event_id"]))
        return {
            "rows": rows,
            "status": "ok" if rows else "empty",
            "meta": {
                "asof": asof.isoformat(),
                "window_days": self.window_days,
                "event_count": len(rows),
            },
        }

    def normalize(self, raw: Mapping[str, Any]) -> Mapping[str, Any]:
        return raw
