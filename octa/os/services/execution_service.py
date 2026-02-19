from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .base import BaseService, ServiceStatus


@dataclass(frozen=True)
class ExecutionServiceConfig:
    default_side: str = "BUY"
    default_qty: float = 1.0


class ExecutionService(BaseService):
    name = "execution_service"

    def __init__(self, cfg: ExecutionServiceConfig) -> None:
        super().__init__()
        self._cfg = cfg
        self._sent_count = 0

    def build_order_intent(
        self,
        *,
        order_id: str,
        symbol: str,
        model_ref: str,
        eligibility_ref: str,
        config_hash: str,
        risk_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "order_id": order_id,
            "symbol": symbol,
            "side": self._cfg.default_side,
            "qty": float(self._cfg.default_qty),
            "price": None,
            "model_ref": model_ref,
            "eligibility_ref": eligibility_ref,
            "config_hash": config_hash,
            "risk_snapshot": risk_snapshot,
        }

    def mark_sent(self) -> None:
        self._sent_count += 1

    def status(self) -> ServiceStatus:
        return ServiceStatus(
            name=self.name,
            started=self._started,
            healthy=True,
            detail="running" if self._started else "stopped",
            metadata={"sent_count": self._sent_count},
        )
