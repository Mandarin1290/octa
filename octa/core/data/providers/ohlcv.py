from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol, Sequence

Timeframe = Literal["1D", "30M", "1H", "5M", "1M"]


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
