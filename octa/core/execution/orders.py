from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class OrderStatus(str, Enum):
    NEW = "NEW"
    SUBMITTED = "SUBMITTED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class OrderRequest:
    symbol: str
    side: str
    qty: float
    order_type: str
    limit_price: float | None = None
    time_in_force: str = "DAY"
    expected_price: float | None = None
    order_id: str | None = None


@dataclass(frozen=True)
class OrderUpdate:
    order_id: str
    status: OrderStatus
    filled_qty: float
    remaining_qty: float
    timestamp: datetime


@dataclass(frozen=True)
class ExecutionReport:
    order_id: str
    symbol: str
    side: str
    qty: float
    filled_qty: float
    avg_fill_price: float
    status: OrderStatus
    slippage: float
    latency_ms: int
    fills: list[dict[str, float]]
