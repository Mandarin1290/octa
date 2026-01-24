from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .types import Identifier, Timestamp


@dataclass(frozen=True)
class Event:
    id: Identifier
    name: str
    timestamp: Timestamp
    payload: dict[str, Any]
