"""Economic calendar features for FX assets.

Expected payload keys:
    eco_surprise_score   — composite economic surprise index (float)
    upcoming_event_count — number of high-impact events in next 24h (int)
    rate_differential    — interest rate differential (base - quote, float)
"""
from __future__ import annotations

import math
from typing import Any, Dict, Mapping, Optional


def build(
    payloads: Mapping[str, Any],
    *,
    asof_ts: Optional[str] = None,
) -> Dict[str, float]:
    """Build economic calendar features. Returns {} if payload keys absent."""
    result: Dict[str, float] = {}
    _safe(payloads, "eco_surprise_score", result, "eco_calendar.surprise_score")
    _safe(payloads, "upcoming_event_count", result, "eco_calendar.upcoming_events")
    _safe(payloads, "rate_differential", result, "eco_calendar.rate_differential")
    return result


def _safe(src: Mapping[str, Any], key: str, dst: Dict[str, float], out_key: str) -> None:
    val = src.get(key)
    if val is None:
        return
    try:
        f = float(val)
        if math.isfinite(f):
            dst[out_key] = f
    except (TypeError, ValueError):
        pass
