from __future__ import annotations

from typing import Protocol

from octa_sentinel.engine import SentinelEngine

from .models import OrderIntent


class PreTradeChecker(Protocol):
    def check(self, intent: OrderIntent) -> bool: ...


class SentinelPreTrade:
    def __init__(self, sentinel: SentinelEngine):
        self.sentinel = sentinel

    def check(self, intent: OrderIntent) -> bool:
        # sentinel exposes evaluate() which returns a dict of gate results
        res = self.sentinel.evaluate(
            {"symbol": intent.symbol, "qty": intent.qty, "price": intent.price}
        )
        # fail-closed: Decision.level > 0 indicates a blocking condition
        return res.level == 0
