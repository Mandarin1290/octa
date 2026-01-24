from __future__ import annotations

from datetime import datetime
from typing import Sequence

from .ohlcv import OHLCVBar


def align_window(
    bars: Sequence[OHLCVBar], end: datetime | None, lookback: int
) -> list[OHLCVBar]:
    if not bars or lookback <= 0:
        return []
    cutoff = end or bars[-1].ts
    filtered = [bar for bar in bars if bar.ts <= cutoff]
    return list(filtered[-lookback:])


def validate_monotonic(bars: Sequence[OHLCVBar]) -> bool:
    if not bars:
        return True
    last = bars[0].ts
    for bar in bars[1:]:
        if bar.ts < last:
            return False
        last = bar.ts
    return True


def detect_missing(
    bars: Sequence[OHLCVBar], expected_step_seconds: int
) -> tuple[bool, bool]:
    if len(bars) < 2:
        return False, False
    missing = False
    gap_risk = False
    for prev, current in zip(bars, bars[1:]):
        delta = (current.ts - prev.ts).total_seconds()
        if delta > expected_step_seconds * 1.5:
            missing = True
        if delta > expected_step_seconds * 3:
            gap_risk = True
    return missing, gap_risk
