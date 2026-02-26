from __future__ import annotations

from typing import Any


def as_float(x: Any, default: float = 0.0) -> float:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def as_int(x: Any, default: int = 0) -> int:
    if x is None:
        return default
    try:
        return int(x)
    except (TypeError, ValueError):
        return default
