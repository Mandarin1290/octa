from __future__ import annotations

import signal
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from octa.execution.risk_engine import RiskEngine

from .capabilities import Capability, CapabilityEnforcer, CapabilityViolation
from .evidence import EvidenceWriter
from .policy_loader import PolicyLoadResult, load_policy
from .runbooks import BrainState, RunbookDecision, decide_runbook
from .sensors import SensorSnapshot, collect_sensors
from .services import (
    AlertsService,
    BrokerService,
    BrokerServiceConfig,
    DashboardService,
    ExecutionService,
    ExecutionServiceConfig,
    TrainingService,
    TrainingServiceConfig,
)
from .state_store import OSStateStore
from .two_phase_commit import TwoPhaseCommitEngine
from .utils import stable_sha256, utc_now_iso

_WAIT_STATES = {
    BrainState.WAIT.value,
    BrainState.WAIT_FOR_BROKER.value,
    BrainState.WAIT_FOR_ELIGIBLE.value,
}
_FORBIDDEN_WAIT_ACTIONS = {"execute_2pc", "send_order", "commit_send"}


@dataclass(frozen=True)
class OSBrainConfig:
    config_path: Path
    policy_path: Path
    mode: str
    arm_live_flag: bool


class OSBrain:
    def __init__(self, cfg: OSBrainConfig, state_store: OSStateStore | None = None) -> None:
        self.cfg = cfg
        self.state_store = state_store or OSStateStore()
        self.policy_result: PolicyLoadResult = load_policy(cfg.policy_path)
        self.policy = self.policy_result.policy

        registry = self.state_store.load_registry()
        default_run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.run_id = str(registry.get("run_id") or f"octa_os_{default_run}")
        self.tick_index = int(registry.get("tick_index", 0))
        self.error_streak = int(registry.get("error_streak", 0))
        last_state = str(registry.get("last_state", BrainState.INIT.value))
        if last_state in BrainState._value2member_map_:
            self.state = BrainState(last_state)
        else:
            self.state = BrainState.INIT

        self.mode = str(cfg.mode or self.policy.get("mode", {}).get("default", "shadow")).lower()
        if self.mode not in {"shadow", "paper", "live"}:
            self.mode = "shadow"

        self.config_hash = stable_sha256(
            {"config_path": str(cfg.config_path), "policy_path": str(cfg.policy_path)}
        )
        self.evidence = EvidenceWriter(self.state_store.paths.evidence_root, self.run_id)
        self.enforcer = CapabilityEnforcer.from_policy(self.policy.get("capabilities", {}))

        broker_mode = "dry-run" if self.mode == "shadow" else self.mode
        live_enabled = self.mode == "live" and cfg.arm_live_flag

        training_cfg = (self.policy.get("services") or {}).get("training") or {}
        self.dashboard_service = DashboardService()
        self.alerts_service = AlertsService(self.state_store.paths.state_root / "alerts.jsonl")
        self.broker_service = BrokerService(
            BrokerServiceConfig(
                mode=broker_mode,
                enable_live=live_enabled,
                i_understand_live_risk=live_enabled,
            )
        )
        self.training_service = TrainingService(
            TrainingServiceConfig(command=str(training_cfg.get("command", "")))
        )
        self.execution_service = ExecutionService(ExecutionServiceConfig())
        self.risk_engine = RiskEngine()
        self.two_pc = TwoPhaseCommitEngine(self.state_store, self.risk_engine)

        self._halt = False
        self._install_signal_handlers()

    def _install_signal_handlers(self) -> None:
        def _handle(_sig: int, _frame: Any) -> None:
            self._halt = True

        signal.signal(signal.SIGINT, _handle)
        signal.signal(signal.SIGTERM, _handle)

    def _require(self, service: str, capability: Capability) -> None:
        self.enforcer.require(service, capability)

    def _start_services(self) -> dict[str, Any]:
        details: dict[str, Any] = {}
        services_cfg = self.policy.get("services", {})
        if not isinstance(services_cfg, dict):
            services_cfg = {}

        if bool((services_cfg.get("dashboard") or {}).get("enabled", True)):
            self._require("dashboard_service", Capability.DASHBOARD_START)
            details["dashboard"] = self.dashboard_service.start().__dict__

        if bool((services_cfg.get("alerts") or {}).get("enabled", True)):
            self._require("alerts_service", Capability.ALERT_SEND)
            details["alerts"] = self.alerts_service.start().__dict__

        if bool((services_cfg.get("broker") or {}).get("enabled", False)):
            self._require("broker_service", Capability.BROKER_CONNECT)
            details["broker"] = self.broker_service.start().__dict__

        if bool((services_cfg.get("training") or {}).get("enabled", True)):
            details["training"] = self.training_service.start().__dict__

        if bool((services_cfg.get("execution") or {}).get("enabled", True)):
            details["execution"] = self.execution_service.start().__dict__

        return details

    def _service_health(self) -> dict[str, Any]:
        return {
            "dashboard": self.dashboard_service.status().__dict__,
            "alerts": self.alerts_service.status().__dict__,
            "broker": self.broker_service.status().__dict__,
            "training": self.training_service.status().__dict__,
            "execution": self.execution_service.status().__dict__,
        }

    def _sensors(self) -> SensorSnapshot:
        return collect_sensors(
            policy=self.policy,
            mode=self.mode,
            policy_valid=self.policy_result.valid,
            blessed_registry=self.state_store.paths.blessed_registry,
        )

    def _safe_mode_for_commit(self, sensors: SensorSnapshot) -> bool:
        if self.mode != "live":
            return True
        return bool(self.cfg.arm_live_flag and sensors.live_armed)

    def _execute_2pc(self, sensors: SensorSnapshot) -> dict[str, Any]:
        eligible = sorted(sensors.eligibility.eligible_symbols)
        if not eligible:
            return {"sent": False, "reason": "no_eligible_symbols"}

        symbol = eligible[0]
        order_id = f"{self.run_id}_{self.tick_index:06d}_{symbol}"

        self.state = BrainState.COMMIT_PHASE_1
        self._require("execution_service", Capability.ISSUE_ORDER_INTENT)
        intent = self.execution_service.build_order_intent(
            order_id=order_id,
            symbol=symbol,
            model_ref=f"blessed:{symbol}",
            eligibility_ref=self.state_store.paths.blessed_registry.as_posix(),
            config_hash=self.config_hash,
            risk_snapshot={"pre_check": "pending"},
        )
        intent_path = self.two_pc.phase1_intent(order_id, intent)

        self.state = BrainState.COMMIT_PHASE_2
        self._require("risk_service", Capability.APPROVE_ORDER)
        approval_path, approved, approval = self.two_pc.phase2_approve(
            order_id,
            nav=100000.0,
            scaling_level=0,
            current_gross=0.0,
        )

        self.state = BrainState.COMMIT_SEND
        sent = False
        reason = "risk_rejected"
        broker_result: dict[str, Any] | None = None

        if approved:
            self._require("execution_service", Capability.SEND_ORDER)
            commit_ok, reason, broker_result = self.two_pc.commit(
                order_id=order_id,
                sensors_ok=(sensors.broker_ready and sensors.data_ready and sensors.risk_ok),
                live_commit_ok=self._safe_mode_for_commit(sensors),
                send_fn=lambda order: self.broker_service.place_order("ml", order),
            )
            sent = bool(commit_ok)
            if sent:
                self.execution_service.mark_sent()

        return {
            "order_id": order_id,
            "symbol": symbol,
            "intent_path": intent_path,
            "approval_path": approval_path,
            "approved": approved,
            "approval_reason": approval.get("reason"),
            "sent": sent,
            "reason": reason,
            "broker_result": broker_result,
        }

    def _validate_tick_invariants(
        self, tick_payload: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        state = str(tick_payload.get("state", ""))
        next_action = str(tick_payload.get("next_action", ""))
        two_pc = tick_payload.get("2pc_status", {})
        action_result = tick_payload.get("action_result", {})

        sent = bool((two_pc or {}).get("sent", False) or (action_result or {}).get("sent", False))
        reasons: list[str] = []

        if state in _WAIT_STATES and sent:
            reasons.append("wait_state_cannot_report_sent")
        if state in _WAIT_STATES and next_action in _FORBIDDEN_WAIT_ACTIONS:
            reasons.append("wait_state_forbidden_next_action")
        if sent and state not in {BrainState.COMMIT_SEND.value, BrainState.EXECUTION_TICK.value}:
            reasons.append("sent_requires_commit_or_execution_state")

        if not reasons:
            return tick_payload, False

        patched = dict(tick_payload)
        patched["state"] = BrainState.RECOVER_BACKOFF.value
        patched["next_action"] = "sleep"
        patched["reason"] = "tick_invariant_violation"
        patched.setdefault("runbook_details", {})
        patched["runbook_details"] = dict(patched.get("runbook_details") or {})
        patched["runbook_details"]["invariant_violations"] = sorted(reasons)

        safe_action = dict(patched.get("action_result") or {})
        safe_action["sent"] = False
        safe_action["reason"] = "invariant_blocked"
        patched["action_result"] = safe_action

        safe_2pc = dict(patched.get("2pc_status") or {})
        safe_2pc["sent"] = False
        safe_2pc["reason"] = "invariant_blocked"
        patched["2pc_status"] = safe_2pc

        patched.setdefault("errors_and_backoff", {})
        patched["errors_and_backoff"] = dict(patched.get("errors_and_backoff") or {})
        patched["errors_and_backoff"]["invariant_violation"] = sorted(reasons)
        return patched, True

    def tick(self) -> dict[str, Any]:
        self.tick_index += 1
        ts_compact = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        chosen: RunbookDecision | None = None
        action_result: dict[str, Any] = {}

        try:
            self.state = BrainState.START_SERVICES
            self._start_services()

            self.state = BrainState.SENSE
            sensors = self._sensors()

            self.state = BrainState.RUNBOOK_DECIDE
            if not self.policy_result.valid:
                cadence = (self.policy.get("cadence") or {}).get("tick_seconds", 30)
                chosen = RunbookDecision(
                    runbook="PolicyInvalidRunbook",
                    next_state=BrainState.WAIT,
                    action="execution_disabled",
                    reason="policy_invalid",
                    next_check_in_sec=int(cadence),
                    details={"errors": list(self.policy_result.errors)},
                )
            else:
                chosen = decide_runbook(
                    sensors=sensors,
                    policy=self.policy,
                    mode=self.mode,
                    error_streak=self.error_streak,
                )

            self.state = chosen.next_state
            if chosen.next_state == BrainState.TRAINING_TICK:
                self._require("training_service", Capability.WRITE_CANDIDATE_MODEL)
                action_result = self.training_service.trigger_tick()
                self.state = BrainState.WAIT_FOR_ELIGIBLE
            elif chosen.next_state == BrainState.EXECUTION_TICK:
                action_result = self._execute_2pc(sensors)
                if bool(action_result.get("sent", False)):
                    self.state = BrainState.COMMIT_SEND
                else:
                    self.state = BrainState.EXECUTION_TICK
            elif chosen.next_state in {
                BrainState.WAIT,
                BrainState.WAIT_FOR_BROKER,
                BrainState.WAIT_FOR_ELIGIBLE,
                BrainState.RECOVER_BACKOFF,
            }:
                action_result = {"wait_reason": chosen.reason, "sent": False}

            self.error_streak = 0
        except CapabilityViolation as exc:
            self.error_streak += 1
            self.state = BrainState.WAIT
            sensors = self._sensors()
            cadence = (self.policy.get("cadence") or {}).get("tick_seconds", 30)
            chosen = RunbookDecision(
                runbook="CapabilityViolationRunbook",
                next_state=BrainState.WAIT,
                action="fail_closed",
                reason=str(exc),
                next_check_in_sec=int(cadence),
                details={},
            )
            action_result = {"error": str(exc), "sent": False}
            try:
                self.alerts_service.send(
                    "ERROR",
                    "Capability violation fail-closed",
                    {"error": str(exc)},
                )
            except Exception:
                pass
        except Exception as exc:
            self.error_streak += 1
            self.state = BrainState.WAIT
            sensors = self._sensors()
            cadence = (self.policy.get("cadence") or {}).get("tick_seconds", 30)
            chosen = RunbookDecision(
                runbook="ExceptionRunbook",
                next_state=BrainState.RECOVER_BACKOFF,
                action="fail_closed",
                reason=f"{type(exc).__name__}:{exc}",
                next_check_in_sec=int(cadence),
                details={},
            )
            action_result = {"error": f"{type(exc).__name__}:{exc}", "sent": False}
            try:
                self.alerts_service.send(
                    "ERROR",
                    "Brain tick exception",
                    {"error": action_result["error"]},
                )
            except Exception:
                pass

        assert chosen is not None
        sensors_snapshot = self._sensors() if "sensors" not in locals() else sensors

        tick_payload = {
            "ts_utc": utc_now_iso(),
            "tick_index": self.tick_index,
            "run_id": self.run_id,
            "state": self.state.value,
            "chosen_runbook": chosen.runbook,
            "next_action": chosen.action,
            "reason": chosen.reason,
            "next_check_in_sec": int(chosen.next_check_in_sec),
            "mode": self.mode,
            "policy_valid": self.policy_result.valid,
            "policy_errors": list(self.policy_result.errors),
            "policy_hash": self.policy_result.policy_hash,
            "config_hash": self.config_hash,
            "eligibility_summary": {
                "registry_exists": sensors_snapshot.eligibility.registry_exists,
                "eligible_symbols": sorted(sensors_snapshot.eligibility.eligible_symbols),
                "reason": sensors_snapshot.eligibility.reason,
            },
            "sensors": {
                "broker_ready": sensors_snapshot.broker_ready,
                "data_ready": sensors_snapshot.data_ready,
                "risk_ok": sensors_snapshot.risk_ok,
                "execution_mode_allowed": sensors_snapshot.execution_mode_allowed,
                "live_armed": sensors_snapshot.live_armed,
                "unknown_flags": list(sensors_snapshot.unknown_flags),
            },
            "service_health": self._service_health(),
            "capabilities": {
                "brain": self.enforcer.service_caps("brain"),
                "dashboard_service": self.enforcer.service_caps("dashboard_service"),
                "alerts_service": self.enforcer.service_caps("alerts_service"),
                "broker_service": self.enforcer.service_caps("broker_service"),
                "training_service": self.enforcer.service_caps("training_service"),
                "execution_service": self.enforcer.service_caps("execution_service"),
                "risk_service": self.enforcer.service_caps("risk_service"),
            },
            "runbook_details": dict(chosen.details),
            "action_result": action_result,
            "errors_and_backoff": {
                "error_streak": self.error_streak,
                "backoff_schedule": list((self.policy.get("backoff") or {}).get("seconds", [])),
            },
            "2pc_status": {
                "order_id": action_result.get("order_id"),
                "approved": action_result.get("approved"),
                "sent": action_result.get("sent"),
                "reason": action_result.get("reason"),
                "intent_path": action_result.get("intent_path"),
                "approval_path": action_result.get("approval_path"),
            },
        }

        tick_payload, invariant_violation = self._validate_tick_invariants(tick_payload)
        if invariant_violation:
            self.error_streak += 1
            self.state = BrainState.RECOVER_BACKOFF
            tick_payload["state"] = self.state.value

        ev = self.evidence.write_tick(ts_compact, tick_payload)
        chain_node = self.state_store.append_chain(
            {
                "tick_index": self.tick_index,
                "run_id": self.run_id,
                "evidence_path": str(ev.path),
                "evidence_hash": ev.payload_hash,
                "state": self.state.value,
                "runbook": chosen.runbook,
            }
        )

        self.state_store.save_registry(
            {
                "run_id": self.run_id,
                "last_state": self.state.value,
                "last_tick_ts": utc_now_iso(),
                "last_tick_path": str(ev.path),
                "tick_index": self.tick_index,
                "chain_head_hash": chain_node["hash"],
                "chain_last_index": chain_node["index"],
                "error_streak": self.error_streak,
                "mode": self.mode,
            }
        )

        return {
            "tick_index": self.tick_index,
            "state": self.state.value,
            "runbook": chosen.runbook,
            "next_check_in_sec": int(chosen.next_check_in_sec),
            "evidence": str(ev.path),
            "action_result": dict(tick_payload.get("action_result") or {}),
        }

    def should_halt(self) -> bool:
        return self._halt
