from __future__ import annotations

from dataclasses import dataclass

from ..orders import OrderRequest


@dataclass(frozen=True)
class IBKROrder:
    contract: object
    order: object
    request: OrderRequest
