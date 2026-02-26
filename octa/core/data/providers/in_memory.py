from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from .ohlcv import OHLCVBar, OHLCVProvider, Timeframe


@dataclass
class InMemoryOHLCVProvider(OHLCVProvider):
    _store: dict[tuple[str, Timeframe], list[OHLCVBar]] = field(default_factory=dict)

    def set_bars(
        self, symbol: str, timeframe: Timeframe, bars: Iterable[OHLCVBar]
    ) -> None:
        self._store[(symbol, timeframe)] = list(bars)

    def add_bars(
        self, symbol: str, timeframe: Timeframe, bars: Iterable[OHLCVBar]
    ) -> None:
        self._store.setdefault((symbol, timeframe), []).extend(list(bars))

    def get_ohlcv(
        self,
        symbol: str,
        timeframe: Timeframe,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> list[OHLCVBar]:
        bars = list(self._store.get((symbol, timeframe), []))
        if start is not None:
            bars = [bar for bar in bars if bar.ts >= start]
        if end is not None:
            bars = [bar for bar in bars if bar.ts <= end]
        if limit is not None:
            bars = bars[-limit:]
        return bars
