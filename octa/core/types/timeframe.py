from __future__ import annotations

from typing import Literal, TypeAlias, cast

Timeframe: TypeAlias = Literal["1D", "1H", "30M", "5M", "1M"]


def coerce_timeframe(tf: str) -> Timeframe:
    if tf not in ("1D", "1H", "30M", "5M", "1M"):
        raise ValueError(f"invalid timeframe: {tf}")
    return cast(Timeframe, tf)
