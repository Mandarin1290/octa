from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Fill:
    order_id: str
    symbol: str
    qty: float
    price: float
    expected_price: float | None
    slippage: float
    timestamp: datetime
    latency_ms: int
