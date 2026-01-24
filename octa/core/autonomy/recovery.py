from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass
class RecoveryState:
    backoff_index: int = 0


def deterministic_backoff(state: RecoveryState, schedule: list[int]) -> int:
    if not schedule:
        return 0
    seconds = schedule[state.backoff_index % len(schedule)]
    state.backoff_index += 1
    return seconds


def quarantine_manager(quarantined: set[str], symbol: str, max_quarantine: int) -> set[str]:
    updated = set(quarantined)
    if symbol:
        updated.add(symbol)
    if len(updated) > max_quarantine:
        return updated
    return updated


class ResettableProvider(Protocol):
    def reset(self) -> None:
        ...


def provider_reset_hook(provider: Any) -> bool:
    if hasattr(provider, "reset") and callable(getattr(provider, "reset")):
        provider.reset()
        return True
    return False
