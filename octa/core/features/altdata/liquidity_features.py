from __future__ import annotations

from typing import Any, Mapping


def build(payloads: Mapping[str, Any]) -> dict[str, float]:
    stooq = payloads.get("stooq", {})
    fmp = payloads.get("fmp", {})

    liquidity = None
    for src in (stooq, fmp):
        if isinstance(src, dict) and "liquidity" in src:
            liquidity = src.get("liquidity")
            break

    try:
        liquidity_val = float(liquidity) if liquidity is not None else 0.0
    except Exception:
        liquidity_val = 0.0

    return {
        "liquidity_proxy": liquidity_val,
    }
