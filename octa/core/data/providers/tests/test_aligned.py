from __future__ import annotations

from datetime import datetime, timedelta

from octa.core.data.providers.aligned import align_window, detect_missing, validate_monotonic
from octa.core.data.providers.ohlcv import OHLCVBar


def _bars(count: int, step_seconds: int = 60) -> list[OHLCVBar]:
    start = datetime(2024, 1, 1)
    bars: list[OHLCVBar] = []
    for idx in range(count):
        ts = start + timedelta(seconds=idx * step_seconds)
        bars.append(OHLCVBar(ts=ts, open=1, high=2, low=1, close=1.5, volume=100))
    return bars


def test_align_window_respects_lookback() -> None:
    bars = _bars(10)
    aligned = align_window(bars, end=bars[-2].ts, lookback=5)
    assert len(aligned) == 5
    assert aligned[-1].ts == bars[-2].ts


def test_validate_monotonic() -> None:
    bars = _bars(3)
    assert validate_monotonic(bars) is True
    assert validate_monotonic(list(reversed(bars))) is False


def test_detect_missing_and_gap() -> None:
    bars = _bars(3, step_seconds=60)
    missing, gap = detect_missing(bars, expected_step_seconds=60)
    assert missing is False
    assert gap is False

    bars_gap = [bars[0], bars[1]] + [
        OHLCVBar(ts=bars[1].ts + timedelta(seconds=400), open=1, high=2, low=1, close=1.5, volume=100)
    ]
    missing, gap = detect_missing(bars_gap, expected_step_seconds=60)
    assert missing is True
    assert gap is True
