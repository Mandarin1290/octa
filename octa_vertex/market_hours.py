"""Pretrade market-hours checks for the vertex (pretrade)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from octa_core.calendar import registry


def pretrade_check(
    instrument: Dict[str, Any], ts: datetime | None = None
) -> Dict[str, Any]:
    """Check if instrument's venue allows trading at `ts` (UTC). Returns dict with `eligible` and `reason`.

    instrument expected fields: `venue` (string)
    ts: timezone-aware or naive (assumed UTC if naive)
    """
    if ts is None:
        ts = datetime.utcnow()
    if ts.tzinfo is None:
        # assume UTC
        from datetime import timezone

        ts = ts.replace(tzinfo=timezone.utc)

    venue = instrument.get("venue")
    if not venue:
        return {"eligible": False, "reason": "missing_venue"}

    cal = registry.get(venue)
    if cal is None:
        return {"eligible": False, "reason": "missing_calendar"}

    if cal.is_holiday(ts):
        return {"eligible": False, "reason": "holiday"}

    if not cal.is_in_session(ts):
        return {"eligible": False, "reason": "out_of_session"}

    if cal.is_in_do_not_trade(ts):
        return {"eligible": False, "reason": "do_not_trade_window"}

    return {"eligible": True, "reason": "ok"}
