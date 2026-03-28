from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Iterator, Sequence

import pandas as pd


@dataclass(frozen=True)
class PaperMarketEvent:
    timestamp: pd.Timestamp
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class MarketDataAdapter(ABC):
    @abstractmethod
    def iter_events(self) -> Iterator[PaperMarketEvent]:
        raise NotImplementedError


class InMemoryMarketDataAdapter(MarketDataAdapter):
    def __init__(self, events: Sequence[PaperMarketEvent]) -> None:
        ordered = sorted(events, key=lambda item: item.timestamp)
        if not ordered:
            raise ValueError("events must not be empty")
        self._events = tuple(ordered)

    @classmethod
    def from_dataframe(cls, symbol: str, frame: pd.DataFrame) -> "InMemoryMarketDataAdapter":
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("frame must be a pandas DataFrame")
        if frame.empty:
            raise ValueError("frame must not be empty")
        if not isinstance(frame.index, pd.DatetimeIndex):
            raise TypeError("frame must use a DatetimeIndex")
        if not frame.index.is_monotonic_increasing:
            raise ValueError("frame index must be monotonic increasing")

        lowered = {str(column).lower(): column for column in frame.columns}
        missing = [name for name in ("open", "high", "low", "close", "volume") if name not in lowered]
        if missing:
            raise ValueError(f"frame missing required columns: {missing}")

        events = [
            PaperMarketEvent(
                timestamp=ts,
                symbol=symbol,
                open=float(row[lowered["open"]]),
                high=float(row[lowered["high"]]),
                low=float(row[lowered["low"]]),
                close=float(row[lowered["close"]]),
                volume=float(row[lowered["volume"]]),
            )
            for ts, row in frame.iterrows()
        ]
        return cls(events)

    def iter_events(self) -> Iterator[PaperMarketEvent]:
        return iter(self._events)


__all__ = ["InMemoryMarketDataAdapter", "MarketDataAdapter", "PaperMarketEvent"]
