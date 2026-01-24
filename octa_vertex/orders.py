from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from octa_core.types import Identifier, Timestamp
from octa_sentinel.core import RiskBlockedError, Sentinel


class ExecutionError(Exception):
    pass


class OrderStatus(str, Enum):
    NEW = "new"
    PENDING = "pending"
    FILLED = "filled"
    REJECTED = "rejected"


@dataclass
class Order:
    id: Identifier
    symbol: str
    qty: float
    status: OrderStatus = OrderStatus.NEW
    created_at: Timestamp = Timestamp.now()


class ExecutionEngine(Protocol):
    def execute(self, order: Order) -> Order: ...


class RiskAwareEngine:
    def __init__(self, engine_impl: ExecutionEngine) -> None:
        self._impl = engine_impl
        self._sentinel = Sentinel.get_instance()

    def execute(self, order: Order) -> Order:
        try:
            self._sentinel.check(order.id, {"symbol": order.symbol, "qty": order.qty})
        except RiskBlockedError as e:
            order.status = OrderStatus.REJECTED
            raise ExecutionError(f"risk blocked: {e}") from e
        # delegate to underlying implementation
        return self._impl.execute(order)
