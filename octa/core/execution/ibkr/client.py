from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List

try:
    from ib_insync import IB  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    IB = None


@dataclass
class IBKRClientConfig:
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 1
    paper_only: bool = True


class IBKRClient:
    def __init__(self, config: IBKRClientConfig | None = None) -> None:
        self._config = config or IBKRClientConfig()
        self._ib = IB() if IB is not None else None

    def connect(self) -> None:
        if self._ib is None:
            raise RuntimeError("ib_insync not available")
        if not self._config.paper_only:
            raise RuntimeError("live trading disabled; paper_only required")
        self._ib.connect(self._config.host, self._config.port, clientId=self._config.client_id)

    def place_order(self, order: Any) -> Any:
        if self._ib is None:
            raise RuntimeError("ib_insync not available")
        return self._ib.placeOrder(order.contract, order.order)

    def cancel_order(self, order: Any) -> None:
        if self._ib is None:
            raise RuntimeError("ib_insync not available")
        self._ib.cancelOrder(order)

    def get_open_orders(self) -> List[Any]:
        if self._ib is None:
            raise RuntimeError("ib_insync not available")
        return list(self._ib.openOrders())

    def get_fills(self) -> List[Any]:
        if self._ib is None:
            raise RuntimeError("ib_insync not available")
        return list(self._ib.fills())
