from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ServiceStatus:
    name: str
    started: bool
    healthy: bool
    detail: str
    metadata: dict[str, Any]


class BaseService:
    name: str = "service"

    def __init__(self) -> None:
        self._started = False

    def start(self) -> ServiceStatus:
        self._started = True
        return self.status()

    def stop(self) -> ServiceStatus:
        self._started = False
        return self.status()

    def status(self) -> ServiceStatus:
        return ServiceStatus(
            name=self.name,
            started=self._started,
            healthy=self._started,
            detail="started" if self._started else "stopped",
            metadata={},
        )
