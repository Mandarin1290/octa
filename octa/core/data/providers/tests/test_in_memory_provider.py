from __future__ import annotations

from datetime import datetime, timedelta

from octa.core.data.providers.in_memory import InMemoryOHLCVProvider
from octa.core.data.providers.ohlcv import OHLCVBar


def test_in_memory_provider_store_and_get() -> None:
    provider = InMemoryOHLCVProvider()
    start = datetime(2024, 1, 1)
    bars = [
        OHLCVBar(ts=start + timedelta(minutes=i), open=1, high=2, low=1, close=1.5, volume=100)
        for i in range(3)
    ]
    provider.set_bars("AAA", "1M", bars)

    fetched = provider.get_ohlcv("AAA", "1M")
    assert fetched == bars


def test_in_memory_provider_timeframe_separation() -> None:
    provider = InMemoryOHLCVProvider()
    bar_1m = OHLCVBar(ts=datetime(2024, 1, 1), open=1, high=2, low=1, close=1.5, volume=100)
    bar_5m = OHLCVBar(ts=datetime(2024, 1, 1), open=1, high=2, low=1, close=1.6, volume=100)
    provider.set_bars("AAA", "1M", [bar_1m])
    provider.set_bars("AAA", "5M", [bar_5m])

    assert provider.get_ohlcv("AAA", "1M") != provider.get_ohlcv("AAA", "5M")
