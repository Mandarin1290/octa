from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Protocol

from octa_core.types import Identifier


class RiskBlockedError(Exception):
    pass


class RiskRule(Protocol):
    def evaluate(self, context: dict) -> bool:
        """Return True to allow, False to block."""


@dataclass
class DefaultAllowAllRule:
    def evaluate(self, context: dict) -> bool:
        return True


class Sentinel:
    _instance: "Sentinel" | None = None
    _lock = Lock()

    def __init__(self) -> None:
        self._enabled = True
        self._rules: list[RiskRule] = [DefaultAllowAllRule()]

    @classmethod
    def get_instance(cls) -> "Sentinel":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = Sentinel()
        return cls._instance

    def is_enabled(self) -> bool:
        return self._enabled

    def set_enabled(self, enabled: bool) -> None:
        self._enabled = enabled

    def add_rule(self, rule: RiskRule) -> None:
        self._rules.append(rule)

    def check(self, entity_id: Identifier, context: dict) -> None:
        if not self._enabled:
            # disabled sentinel => allow
            return
        for r in self._rules:
            try:
                ok = r.evaluate(context)
            except Exception:
                # failure in evaluation => block conservatively
                raise RiskBlockedError("risk evaluation failed") from None
            if not ok:
                raise RiskBlockedError("blocked by risk rule")
