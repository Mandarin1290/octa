"""Futures basis features for the structure_30m gate layer.

Wraps octa_assets.futures.basis compute_basis() and basis_history_metrics().

Expected payload keys:
    futures_close    — back-adjusted futures close (float)
    spot_close       — spot price; None or absent → skip basis features
    multiplier       — contract multiplier (float, default 1.0)
    basis_history    — list of recent basis values for rolling stats (List[float])
    roll_days_remaining — calendar days until next roll (int, optional)
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Mapping, Optional

from octa_assets.futures.basis import basis_history_metrics, compute_basis


def build(
    payloads: Mapping[str, Any],
    *,
    asof_ts: Optional[str] = None,
) -> Dict[str, float]:
    """Build futures basis features.

    Returns {} if spot_close is unavailable or any value is non-finite.
    """
    result: Dict[str, float] = {}

    futures_close = _float(payloads.get("futures_close"))
    spot_close = _float(payloads.get("spot_close"))
    multiplier = _float(payloads.get("multiplier")) or 1.0

    if futures_close is None or spot_close is None:
        return result  # cannot compute basis without both prices

    current_basis = compute_basis(futures_close, spot_close, multiplier)
    if not math.isfinite(current_basis):
        return result

    result["basis.current"] = current_basis

    history_raw = payloads.get("basis_history")
    if isinstance(history_raw, (list, tuple)) and history_raw:
        history: List[float] = [x for x in ((_float(v)) for v in history_raw) if x is not None and math.isfinite(x)]
        if history:
            stats = basis_history_metrics(history)
            mean = stats.get("mean", 0.0)
            std = stats.get("std", 0.0)
            if math.isfinite(mean):
                result["basis.mean"] = mean
            if math.isfinite(std) and std > 0.0:
                result["basis.std"] = std
                z = (current_basis - mean) / std
                if math.isfinite(z):
                    result["basis.z_score"] = z

    roll_days = payloads.get("roll_days_remaining")
    if roll_days is not None:
        try:
            rd = int(roll_days)
            result["basis.roll_days_remaining"] = float(rd)
        except (TypeError, ValueError):
            pass

    return result


def _float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None
