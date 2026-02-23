"""Options Greeks aggregation features for the structure_30m gate layer.

Expected payload keys (all optional):
    delta_mean, delta_std        — aggregate delta across option chain
    gamma_mean                   — aggregate gamma
    vega_mean                    — aggregate vega (vol sensitivity)
    theta_mean                   — aggregate theta (time decay)
    put_call_ratio               — ratio of put to call OI or volume
    skew_25d                     — 25-delta risk reversal (put IV - call IV)
"""
from __future__ import annotations

import math
from typing import Any, Dict, Mapping, Optional


def build(
    payloads: Mapping[str, Any],
    *,
    asof_ts: Optional[str] = None,
) -> Dict[str, float]:
    """Build Greeks aggregation features. Returns {} if payload keys absent."""
    result: Dict[str, float] = {}
    _safe(payloads, "delta_mean", result, "greeks.delta_mean")
    _safe(payloads, "delta_std", result, "greeks.delta_std")
    _safe(payloads, "gamma_mean", result, "greeks.gamma_mean")
    _safe(payloads, "vega_mean", result, "greeks.vega_mean")
    _safe(payloads, "theta_mean", result, "greeks.theta_mean")
    _safe(payloads, "put_call_ratio", result, "greeks.put_call_ratio")
    _safe(payloads, "skew_25d", result, "greeks.skew_25d")
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
