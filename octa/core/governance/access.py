from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class AccessRole(str, Enum):
    READ_ONLY = "READ_ONLY"
    TRADER = "TRADER"
    ADMIN = "ADMIN"


@dataclass(frozen=True)
class AccessPolicy:
    def allow(self, role: AccessRole, action: str) -> bool:
        if role == AccessRole.ADMIN:
            return True
        if role == AccessRole.TRADER:
            return action in {"trade", "read"}
        return action == "read"
