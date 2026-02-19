from __future__ import annotations

from .base import BaseService, ServiceStatus


class DashboardService(BaseService):
    name = "dashboard_service"

    def status(self) -> ServiceStatus:
        return ServiceStatus(
            name=self.name,
            started=self._started,
            healthy=True,
            detail="running" if self._started else "stopped",
            metadata={"mode": "local_wrapper"},
        )
