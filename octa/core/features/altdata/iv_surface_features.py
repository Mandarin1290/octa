"""Implied volatility surface features for the signal_1h gate layer.

Expected payload keys (all optional):
    iv_atm            — at-the-money IV (float, annualised)
    iv_term_slope     — slope of IV term structure (e.g. 1M vs 3M IV difference)
    iv_skew           — put-call IV skew (25d RR)
    iv_percentile_1y  — current IV percentile vs 1-year history (0-1)
    vix               — VIX or equivalent volatility index
"""
from __future__ import annotations

import math
from typing import Any, Dict, Mapping, Optional


def build(
    payloads: Mapping[str, Any],
    *,
    asof_ts: Optional[str] = None,
) -> Dict[str, float]:
    """Build IV surface features. Returns {} if payload keys absent."""
    result: Dict[str, float] = {}
    _safe(payloads, "iv_atm", result, "iv_surface.atm")
    _safe(payloads, "iv_term_slope", result, "iv_surface.term_slope")
    _safe(payloads, "iv_skew", result, "iv_surface.skew")
    _safe(payloads, "iv_percentile_1y", result, "iv_surface.percentile_1y")
    _safe(payloads, "vix", result, "iv_surface.vix")
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
