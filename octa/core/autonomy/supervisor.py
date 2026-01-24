from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Sequence

from octa.core.data.providers.ohlcv import OHLCVProvider
from octa.core.governance.audit_chain import AuditChain

from .events import AutonomyEvent, make_event, write_autonomy_event
from .health import (
    HealthLevel,
    HealthReport,
    check_audit_writable,
    check_provider,
    check_recent_errors,
    summarize_health,
)
from .recovery import RecoveryState, deterministic_backoff, provider_reset_hook, quarantine_manager
from .runbooks import (
    RunbookConfig,
    RunbookPlan,
    RunbookActionType,
    audit_failure_runbook,
    oms_failure_runbook,
    select_runbook,
)


@dataclass(frozen=True)
class SupervisorConfig:
    mode: str = "NORMAL"
    max_cycles: int = 1
    max_consecutive_failures: int = 2
    backoff_schedule_s: list[int] = (1, 2, 5, 10)
    max_quarantined_symbols: int = 5
    execution_mode: str = "OMS"
    sleep_fn: Callable[[int], None] = time.sleep


@dataclass
class SupervisorState:
    cycle: int = 0
    consecutive_failures: int = 0
    quarantined_symbols: set[str] = field(default_factory=set)
    last_health: HealthReport | None = None
    error_counters: dict[str, int] = field(default_factory=dict)
    execution_mode: str = "OMS"
    recovery: RecoveryState = field(default_factory=RecoveryState)


class AutonomySupervisor:
    def __init__(
        self,
        *,
        audit_dir: Path,
        provider: OHLCVProvider,
        run_pipeline: Callable[..., object],
        audit_chain: AuditChain | None = None,
        config: SupervisorConfig | None = None,
    ) -> None:
        self._audit_dir = audit_dir
        self._provider = provider
        self._run_pipeline = run_pipeline
        self._audit_chain = audit_chain
        self._config = config or SupervisorConfig()
        self._state = SupervisorState(execution_mode=self._config.execution_mode)
        self._runbook_cfg = RunbookConfig(
            backoff_schedule_s=list(self._config.backoff_schedule_s),
            max_quarantined_symbols=self._config.max_quarantined_symbols,
        )

    def run(self, *, symbols: list[str], start: datetime | None, end: datetime | None) -> None:
        for _ in range(self._config.max_cycles):
            self._state.cycle += 1
            event_path = self._audit_dir / "autonomy_events.jsonl"
            self._emit_event(make_event("RUN_START", self._config.mode, {"cycle": self._state.cycle}), event_path)

            report = self._build_health_report(symbols)
            self._state.last_health = report

            if report.overall == HealthLevel.CRITICAL:
                plan = select_runbook(
                    report,
                    symbols[0] if symbols else "",
                    self._state.quarantined_symbols,
                    self._runbook_cfg,
                    allow_provider_reset=hasattr(self._provider, "reset"),
                )
                if plan:
                    self._apply_runbook(plan, event_path)
                    if plan.halt:
                        self._emit_event(
                            make_event("HALT", "SAFE", {"reason": "CRITICAL_HEALTH"}, level="ERROR"),
                            event_path,
                        )
                        break

            active_symbols = [symbol for symbol in symbols if symbol not in self._state.quarantined_symbols]

            try:
                self._run_pipeline(
                    active_symbols,
                    self._provider,
                    start,
                    end,
                    self._audit_dir / "paper_run.jsonl",
                    execution_mode=self._state.execution_mode,
                    audit_chain=self._audit_chain,
                )
                self._state.consecutive_failures = 0
                self._emit_event(
                    make_event("RUN_SUCCESS", self._config.mode, {"cycle": self._state.cycle}),
                    event_path,
                )
            except Exception as exc:  # pragma: no cover - exercised in tests
                self._state.consecutive_failures += 1
                self._state.error_counters["pipeline"] = self._state.error_counters.get("pipeline", 0) + 1
                self._emit_event(
                    make_event("RUN_FAIL", self._config.mode, {"error": str(exc)}, level="ERROR"),
                    event_path,
                )
                if "OMS" in str(exc):
                    self._state.error_counters["oms"] = self._state.error_counters.get("oms", 0) + 1
                plan = select_runbook(
                    report,
                    symbols[0] if symbols else "",
                    self._state.quarantined_symbols,
                    self._runbook_cfg,
                    allow_provider_reset=hasattr(self._provider, "reset"),
                )
                if self._state.error_counters.get("oms", 0) >= self._runbook_cfg.oms_failure_threshold:
                    plan = oms_failure_runbook()
                if plan:
                    self._apply_runbook(plan, event_path)
                if self._state.consecutive_failures >= self._config.max_consecutive_failures:
                    self._emit_event(
                        make_event("SAFE_MODE", "SAFE", {"reason": "FAILURES"}, level="WARN"),
                        event_path,
                    )
                    break

    def _build_health_report(self, symbols: Sequence[str]) -> HealthReport:
        symbol = symbols[0] if symbols else ""
        data = check_provider(self._provider, symbol, "1D") if symbol else None
        audit = check_audit_writable(self._audit_dir / "paper_run.jsonl")
        cascade = check_recent_errors(self._state.error_counters)
        subsystems = [audit, cascade]
        if data is not None:
            subsystems.insert(0, data)
        return summarize_health(subsystems)

    def _apply_runbook(self, plan: RunbookPlan, event_path: Path) -> None:
        for action in plan.actions:
            self._emit_event(
                make_event("APPLY_RUNBOOK", plan.next_mode, {"action": action.type.value, **action.params}),
                event_path,
            )
            if action.type == RunbookActionType.BACKOFF:
                seconds = deterministic_backoff(self._state.recovery, list(self._config.backoff_schedule_s))
                self._config.sleep_fn(seconds)
            if action.type == RunbookActionType.QUARANTINE_SYMBOL:
                symbol = str(action.params.get("symbol", ""))
                self._state.quarantined_symbols = quarantine_manager(
                    self._state.quarantined_symbols, symbol, self._config.max_quarantined_symbols
                )
            if action.type == RunbookActionType.DISABLE_OMS:
                self._state.execution_mode = str(action.params.get("execution_mode", "DECISIONS_ONLY"))
            if action.type == RunbookActionType.RESET_PROVIDER:
                provider_reset_hook(self._provider)

    def _emit_event(self, event: AutonomyEvent, event_path: Path) -> None:
        try:
            write_autonomy_event(event, audit_chain=self._audit_chain, jsonl_path=event_path)
        except Exception as exc:
            plan = audit_failure_runbook()
            self._apply_runbook(plan, event_path)
            raise RuntimeError("Autonomy audit failure") from exc
