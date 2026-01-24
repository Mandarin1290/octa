from __future__ import annotations

from dataclasses import dataclass, field

from ..state import ExecutionState


@dataclass
class IBKRState:
    connected: bool = False
    execution_state: ExecutionState = field(default_factory=ExecutionState)
