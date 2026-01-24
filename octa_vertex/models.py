from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from octa_core.types import Identifier, Timestamp


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    NEW = "NEW"
    SENT = "SENT"
    ACK = "ACK"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELED = "CANCELED"


@dataclass
class OrderIntent:
    intent_id: str
    symbol: str
    side: OrderSide
    qty: float
    price: Optional[float]
    notional: Optional[float]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Order:
    id: Identifier
    intent_id: str
    symbol: str
    side: OrderSide
    qty: float
    price: Optional[float]
    status: OrderStatus
    filled_qty: float = 0.0
    created_at: Timestamp = field(default_factory=Timestamp.now)


@dataclass
class ExecutionReport:
    order_id: Identifier
    report_id: str
    status: OrderStatus
    filled_qty: float
    remaining_qty: float
    msg: Optional[str] = None
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
