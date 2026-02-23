"""Funding rate features for crypto perpetual futures.

Expected payload keys:
    funding_rate         — current funding rate (float)
    funding_rate_8h_avg  — 8-hour average funding rate
    funding_rate_24h_avg — 24-hour average funding rate
"""
from __future__ import annotations

import math
from typing import Any, Dict, Mapping, Optional


def build(
    payloads: Mapping[str, Any],
    *,
    asof_ts: Optional[str] = None,
) -> Dict[str, float]:
    """Build funding rate features. Returns {} if payload keys absent."""
    result: Dict[str, float] = {}
    _safe(payloads, "funding_rate", result, "funding_rate.current")
    _safe(payloads, "funding_rate_8h_avg", result, "funding_rate.avg_8h")
    _safe(payloads, "funding_rate_24h_avg", result, "funding_rate.avg_24h")
    # Derived: sign of funding (1 = longs pay shorts = bullish pressure)
    if "funding_rate.current" in result:
        result["funding_rate.sign"] = 1.0 if result["funding_rate.current"] >= 0.0 else -1.0
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
