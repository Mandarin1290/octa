from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from octa.execution.broker_router import BrokerRouter, BrokerRouterConfig

from .base import BaseService, ServiceStatus


@dataclass(frozen=True)
class BrokerServiceConfig:
    mode: str = "dry-run"
    enable_live: bool = False
    i_understand_live_risk: bool = False


class BrokerService(BaseService):
    name = "broker_service"

    def __init__(self, cfg: BrokerServiceConfig) -> None:
        super().__init__()
        self._cfg = cfg
        self._router = BrokerRouter(
            BrokerRouterConfig(
                mode=cfg.mode,
                enable_live=cfg.enable_live,
                i_understand_live_risk=cfg.i_understand_live_risk,
            )
        )

    def health(self) -> dict[str, Any]:
        return self._router.health_check()

    def place_order(self, strategy: str, order: dict[str, Any]) -> dict[str, Any]:
        return self._router.place_order(strategy=strategy, order=order)

    def status(self) -> ServiceStatus:
        health = self.health()
        return ServiceStatus(
            name=self.name,
            started=self._started,
            healthy=bool(health.get("ok", False)),
            detail="ready" if health.get("ok", False) else "not_ready",
            metadata=health,
        )
