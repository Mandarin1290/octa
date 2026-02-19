from __future__ import annotations

import os
import shlex
import subprocess
from dataclasses import dataclass
from typing import Any

from .base import BaseService, ServiceStatus


@dataclass(frozen=True)
class TrainingServiceConfig:
    command: str = ""


class TrainingService(BaseService):
    name = "training_service"

    def __init__(self, cfg: TrainingServiceConfig) -> None:
        super().__init__()
        self._cfg = cfg
        self._last_pid: int | None = None

    def is_running(self) -> bool:
        cp = subprocess.run(
            ["pgrep", "-fa", "run_full_cascade_training_from_parquets.py|octa_training.run_daemon"],
            capture_output=True,
            text=True,
            check=False,
        )
        lines = [ln for ln in (cp.stdout or "").splitlines() if "octa_os_start.py" not in ln]
        return len(lines) > 0

    def trigger_tick(self) -> dict[str, Any]:
        if self.is_running():
            return {"triggered": False, "reason": "training_already_running"}

        cmd = str(self._cfg.command).strip()
        if not cmd:
            return {"triggered": False, "reason": "training_command_not_configured"}

        proc = subprocess.Popen(  # noqa: S603
            shlex.split(cmd),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            env=dict(os.environ),
        )
        self._last_pid = int(proc.pid)
        return {"triggered": True, "pid": self._last_pid, "reason": "started"}

    def status(self) -> ServiceStatus:
        running = self.is_running()
        return ServiceStatus(
            name=self.name,
            started=self._started,
            healthy=True,
            detail="running" if running else "idle",
            metadata={"running": running, "last_pid": self._last_pid},
        )
