"""On-chain altdata features for crypto assets.

Implements the builder contract: build(payloads, *, asof_ts) -> Dict[str, float].

Expected payload keys (all optional — returns {} if absent):
    nvt_ratio, exchange_netflow, active_addresses, mvrv_z_score, tvl_usd
    (market-wide keys, asset-class level)

Symbol-level function build_symbol(payloads) is also provided for the
structure_30m gate layer dispatch.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Mapping, Optional


def build(
    payloads: Mapping[str, Any],
    *,
    asof_ts: Optional[str] = None,
) -> Dict[str, float]:
    """Build market-wide on-chain features.

    Returns {} if no relevant payload keys are present.
    All returned values are guaranteed to be finite floats.
    """
    result: Dict[str, float] = {}
    _safe_float(payloads, "nvt_ratio", result, "onchain.nvt_ratio")
    _safe_float(payloads, "exchange_netflow", result, "onchain.exchange_netflow")
    _safe_float(payloads, "active_addresses", result, "onchain.active_addresses")
    _safe_float(payloads, "mvrv_z_score", result, "onchain.mvrv_z_score")
    _safe_float(payloads, "tvl_usd", result, "onchain.tvl_usd")
    return result


def build_symbol(
    payloads: Mapping[str, Any],
    *,
    asof_ts: Optional[str] = None,
) -> Dict[str, float]:
    """Build per-symbol on-chain features for structure_30m gate layer."""
    result: Dict[str, float] = {}
    _safe_float(payloads, "symbol_exchange_netflow", result, "onchain.symbol_exchange_netflow")
    _safe_float(payloads, "symbol_active_addresses", result, "onchain.symbol_active_addresses")
    return result


def _safe_float(src: Mapping[str, Any], key: str, dst: Dict[str, float], out_key: str) -> None:
    val = src.get(key)
    if val is None:
        return
    try:
        f = float(val)
        if math.isfinite(f):
            dst[out_key] = f
    except (TypeError, ValueError):
        pass
