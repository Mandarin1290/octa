from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class ActionSignal:
    level: int
    command: str
    details: Dict[str, Any]


def freeze_new_orders() -> ActionSignal:
    return ActionSignal(level=2, command="FREEZE_NEW_ORDERS", details={})


def derisk_positions(reason: str) -> ActionSignal:
    return ActionSignal(level=1, command="DERISK", details={"reason": reason})


def flatten_and_kill(reason: str) -> ActionSignal:
    return ActionSignal(level=3, command="FLATTEN_AND_KILL", details={"reason": reason})


def warning(reason: str) -> ActionSignal:
    return ActionSignal(level=0, command="WARNING", details={"reason": reason})
