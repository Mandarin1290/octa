from __future__ import annotations

from pathlib import Path

from ..utils import append_jsonl, utc_now_iso
from .base import BaseService, ServiceStatus


class AlertsService(BaseService):
    name = "alerts_service"

    def __init__(self, out_path: Path) -> None:
        super().__init__()
        self._path = out_path

    def send(self, level: str, message: str, payload: dict[str, object] | None = None) -> None:
        append_jsonl(
            self._path,
            {
                "ts_utc": utc_now_iso(),
                "level": str(level).upper(),
                "message": message,
                "payload": payload or {},
            },
        )

    def status(self) -> ServiceStatus:
        return ServiceStatus(
            name=self.name,
            started=self._started,
            healthy=True,
            detail="running" if self._started else "stopped",
            metadata={"alerts_path": str(self._path)},
        )
