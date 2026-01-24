from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import NewType

Identifier = NewType("Identifier", str)


@dataclass(frozen=True)
class Timestamp:
    ts: datetime

    @staticmethod
    def now() -> "Timestamp":
        return Timestamp(datetime.now(timezone.utc))

    def iso(self) -> str:
        return self.ts.isoformat()
