from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class KillSwitchConfig:
    max_execution_failures: int = 3
    max_slippage: float = 0.02
    max_daily_loss: float = 0.05
    min_system_health: float = 0.7


@dataclass(frozen=True)
class KillSwitchState:
    execution_failures: int
    slippage: float
    daily_loss: float
    system_health: float


@dataclass(frozen=True)
class KillSwitchDecision:
    triggered: bool
    reason: str


def evaluate_kill_switch(state: KillSwitchState, config: KillSwitchConfig | None = None) -> KillSwitchDecision:
    cfg = config or KillSwitchConfig()
    if state.execution_failures >= cfg.max_execution_failures:
        return KillSwitchDecision(True, "EXECUTION_FAILURES")
    if state.slippage >= cfg.max_slippage:
        return KillSwitchDecision(True, "SLIPPAGE")
    if state.daily_loss >= cfg.max_daily_loss:
        return KillSwitchDecision(True, "DAILY_LOSS")
    if state.system_health <= cfg.min_system_health:
        return KillSwitchDecision(True, "SYSTEM_HEALTH")
    return KillSwitchDecision(False, "OK")
