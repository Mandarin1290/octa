from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class GateState:
    level: int
    reason: str
    timestamp: str


class StateStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def save(self, state: GateState) -> None:
        with open(self.path, "w", encoding="utf-8") as fh:
            json.dump(state.__dict__, fh, sort_keys=True)

    def load(self) -> Optional[GateState]:
        if not self.path.exists():
            return None
        with open(self.path, "r", encoding="utf-8") as fh:
            d = json.load(fh)
        return GateState(level=d["level"], reason=d["reason"], timestamp=d["timestamp"])

    def current(self) -> GateState:
        s = self.load()
        if s is None:
            return GateState(
                level=0, reason="init", timestamp=datetime.now(timezone.utc).isoformat()
            )
        return s
