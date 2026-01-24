from __future__ import annotations

from dataclasses import dataclass

from ..orders import OrderRequest


@dataclass(frozen=True)
class OrderAdapter:
    def to_request(
        self,
        symbol: str,
        side: str,
        qty: float,
        order_type: str,
        limit_price: float | None,
        expected_price: float | None,
    ) -> OrderRequest:
        return OrderRequest(
            symbol=symbol,
            side=side,
            qty=qty,
            order_type=order_type,
            limit_price=limit_price,
            expected_price=expected_price,
        )
