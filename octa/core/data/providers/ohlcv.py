from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, Sequence

from octa.core.types.timeframe import Timeframe


@dataclass(frozen=True)
class OHLCVBar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class OHLCVProvider(Protocol):
    def get_ohlcv(
        self,
        symbol: str,
        timeframe: Timeframe,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> Sequence[OHLCVBar]:
        ...
