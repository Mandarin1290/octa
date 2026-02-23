"""Commitments of Traders (COT) positioning features for FX assets.

Expected payload keys:
    cot_net_position     — net speculative position (float, can be negative)
    cot_net_position_pct — net position as % of open interest (float)
    cot_change_week      — week-over-week change in net position (float)
"""
from __future__ import annotations

import math
from typing import Any, Dict, Mapping, Optional


def build(
    payloads: Mapping[str, Any],
    *,
    asof_ts: Optional[str] = None,
) -> Dict[str, float]:
    """Build COT positioning features. Returns {} if payload keys absent."""
    result: Dict[str, float] = {}
    _safe(payloads, "cot_net_position", result, "cot.net_position")
    _safe(payloads, "cot_net_position_pct", result, "cot.net_position_pct")
    _safe(payloads, "cot_change_week", result, "cot.change_week")
    # Derived: direction flag (1 = net long, -1 = net short)
    if "cot.net_position" in result:
        result["cot.direction"] = 1.0 if result["cot.net_position"] >= 0.0 else -1.0
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
