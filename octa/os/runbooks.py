from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from .sensors import SensorSnapshot


class BrainState(str, Enum):
    INIT = "INIT"
    START_SERVICES = "START_SERVICES"
    SENSE = "SENSE"
    RUNBOOK_DECIDE = "RUNBOOK_DECIDE"
    WAIT = "WAIT"
    WAIT_FOR_BROKER = "WAIT_FOR_BROKER"
    WAIT_FOR_ELIGIBLE = "WAIT_FOR_ELIGIBLE"
    TRAINING_TICK = "TRAINING_TICK"
    EXECUTION_TICK = "EXECUTION_TICK"
    COMMIT_PHASE_1 = "COMMIT_PHASE_1"
    COMMIT_PHASE_2 = "COMMIT_PHASE_2"
    COMMIT_SEND = "COMMIT_SEND"
    RECOVER_BACKOFF = "RECOVER_BACKOFF"
    HALT = "HALT"


@dataclass(frozen=True)
class RunbookDecision:
    runbook: str
    next_state: BrainState
    action: str
    reason: str
    next_check_in_sec: int
    details: dict[str, Any]


def decide_runbook(
    *,
    sensors: SensorSnapshot,
    policy: dict[str, Any],
    mode: str,
    error_streak: int,
) -> RunbookDecision:
    cadence = policy.get("cadence", {}) if isinstance(policy.get("cadence"), dict) else {}
    backoff = policy.get("backoff", {}) if isinstance(policy.get("backoff"), dict) else {}

    # Priority 1: safety
    if not sensors.risk_ok:
        return RunbookDecision(
            runbook="SafetyRunbook",
            next_state=BrainState.WAIT,
            action="disable_execution",
            reason="risk_not_ok",
            next_check_in_sec=int(cadence.get("execution_check_seconds", 30)),
            details={},
        )

    # Priority 2: broker recovery
    broker_required = bool(
        ((policy.get("services") or {}).get("broker") or {}).get("required_for_execution", False)
    )
    if broker_required and not sensors.broker_ready:
        return RunbookDecision(
            runbook="BrokerRecoveryRunbook",
            next_state=BrainState.WAIT_FOR_BROKER,
            action="retry_broker",
            reason="broker_not_ready",
            next_check_in_sec=int(cadence.get("execution_check_seconds", 30)),
            details={},
        )

    # Priority 3: degradation
    max_errors = int(backoff.get("max_errors_before_degrade", 3))
    if error_streak >= max_errors:
        schedule = [int(x) for x in backoff.get("seconds", [30, 60, 120])]
        wait_s = schedule[min(len(schedule) - 1, max(0, error_streak - max_errors))]
        return RunbookDecision(
            runbook="DegradationRunbook",
            next_state=BrainState.RECOVER_BACKOFF,
            action="increase_backoff",
            reason="error_streak",
            next_check_in_sec=int(wait_s),
            details={"error_streak": error_streak},
        )

    # Priority 4: eligibility / training
    eligible = sensors.eligibility.eligible_symbols
    training_enabled = bool(
        ((policy.get("services") or {}).get("training") or {}).get("enabled", False)
    )
    if not eligible:
        if training_enabled and sensors.training_window_open:
            return RunbookDecision(
                runbook="EligibilityRunbook",
                next_state=BrainState.TRAINING_TICK,
                action="trigger_training_tick",
                reason=sensors.eligibility.reason,
                next_check_in_sec=int(cadence.get("training_check_seconds", 600)),
                details={"eligible_symbols": []},
            )
        return RunbookDecision(
            runbook="EligibilityRunbook",
            next_state=BrainState.WAIT_FOR_ELIGIBLE,
            action="wait_for_blessed",
            reason=sensors.eligibility.reason,
            next_check_in_sec=int(cadence.get("eligibility_check_seconds", 300)),
            details={"eligible_symbols": []},
        )

    # Priority 5: execution
    if sensors.execution_mode_allowed and sensors.data_ready and sensors.risk_ok:
        if mode == "live" and not sensors.live_armed:
            return RunbookDecision(
                runbook="ExecutionRunbook",
                next_state=BrainState.WAIT,
                action="wait_live_arm",
                reason="live_not_armed",
                next_check_in_sec=int(cadence.get("execution_check_seconds", 30)),
                details={},
            )
        return RunbookDecision(
            runbook="ExecutionRunbook",
            next_state=BrainState.EXECUTION_TICK,
            action="execute_2pc",
            reason="eligible_and_safe",
            next_check_in_sec=int(cadence.get("execution_check_seconds", 30)),
            details={"eligible_symbols": list(eligible)},
        )

    return RunbookDecision(
        runbook="BootRunbook",
        next_state=BrainState.WAIT,
        action="wait",
        reason="default_wait",
        next_check_in_sec=int(cadence.get("tick_seconds", 30)),
        details={},
    )
