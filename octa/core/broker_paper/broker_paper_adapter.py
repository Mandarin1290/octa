from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BrokerPaperOrder:
    timestamp: str
    symbol: str
    side: str
    quantity: float
    reference_price: float
    mode: str = "PAPER"


@dataclass(frozen=True)
class BrokerPaperFill:
    timestamp: str
    symbol: str
    side: str
    quantity: float
    fill_price: float
    fee: float
    mode: str = "PAPER"


class BrokerPaperAdapter(ABC):
    def __init__(self, *, mode: str) -> None:
        if mode != "PAPER":
            raise ValueError("broker adapter must run in explicit PAPER mode")
        self.mode = mode

    @abstractmethod
    def submit_order(self, order: BrokerPaperOrder) -> BrokerPaperFill:
        raise NotImplementedError


class InMemoryBrokerPaperAdapter(BrokerPaperAdapter):
    def __init__(self, *, mode: str = "PAPER", fee_rate: float = 0.0, slippage: float = 0.0) -> None:
        super().__init__(mode=mode)
        self.fee_rate = float(fee_rate)
        self.slippage = float(slippage)
        self.orders: list[BrokerPaperOrder] = []
        self.fills: list[BrokerPaperFill] = []

    def submit_order(self, order: BrokerPaperOrder) -> BrokerPaperFill:
        if order.mode != "PAPER":
            raise ValueError("order mode must be PAPER")
        self.orders.append(order)
        adjusted = order.reference_price * (1.0 + self.slippage if order.side == "buy" else 1.0 - self.slippage)
        fee = abs(order.quantity * adjusted) * self.fee_rate
        fill = BrokerPaperFill(
            timestamp=order.timestamp,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            fill_price=adjusted,
            fee=fee,
            mode="PAPER",
        )
        self.fills.append(fill)
        return fill


__all__ = [
    "BrokerPaperAdapter",
    "BrokerPaperFill",
    "BrokerPaperOrder",
    "InMemoryBrokerPaperAdapter",
]
