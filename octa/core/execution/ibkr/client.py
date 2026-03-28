from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List

_IBClass: Any = None
try:
    from ib_insync import IB as _IBClass  # type: ignore[assignment]
except Exception:  # pragma: no cover - optional dependency
    pass

_FOUNDATION_SCOPE_BLOCK_REASON = "real_order_blocked_in_v0_0_0_foundation_scope"


@dataclass
class IBKRClientConfig:
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 1
    paper_only: bool = True


class IBKRClient:
    def __init__(self, config: IBKRClientConfig | None = None) -> None:
        self._config = config or IBKRClientConfig()
        self._ib = _IBClass() if _IBClass is not None else None

    def connect(self) -> None:
        raise RuntimeError(_FOUNDATION_SCOPE_BLOCK_REASON)

    def place_order(self, order: Any) -> Any:
        raise RuntimeError(_FOUNDATION_SCOPE_BLOCK_REASON)

    def cancel_order(self, order: Any) -> None:
        raise RuntimeError(_FOUNDATION_SCOPE_BLOCK_REASON)

    def get_open_orders(self) -> List[Any]:
        raise RuntimeError(_FOUNDATION_SCOPE_BLOCK_REASON)

    def get_fills(self) -> List[Any]:
        raise RuntimeError(_FOUNDATION_SCOPE_BLOCK_REASON)
