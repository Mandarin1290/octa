from __future__ import annotations

import datetime
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class RunbookError(Exception):
    pass


@dataclass
class Runbook:
    incident_type: str
    summary: str
    immediate_actions: List[Dict[str, Any]]
    escalation_chain: List[Dict[str, Any]]
    recovery_steps: List[Dict[str, Any]]
    created_at: str = field(
        default_factory=lambda: datetime.datetime.utcnow().isoformat() + "Z"
    )
    id: str = field(init=False)

    def __post_init__(self):
        # deterministic id from canonical JSON
        self.id = canonical_hash(
            {
                "incident_type": self.incident_type,
                "summary": self.summary,
                "immediate_actions": self.immediate_actions,
                "escalation_chain": self.escalation_chain,
                "recovery_steps": self.recovery_steps,
            }
        )

    def validate(self) -> None:
        # Ensure required top-level fields exist and are non-empty
        if not self.incident_type or not self.summary:
            raise RunbookError("incident_type and summary required")

        if (
            not isinstance(self.immediate_actions, list)
            or len(self.immediate_actions) == 0
        ):
            raise RunbookError(
                "immediate_actions must be a non-empty list of actionable steps"
            )

        if (
            not isinstance(self.escalation_chain, list)
            or len(self.escalation_chain) == 0
        ):
            raise RunbookError(
                "escalation_chain must be a non-empty list of contacts/roles"
            )

        if not isinstance(self.recovery_steps, list) or len(self.recovery_steps) == 0:
            raise RunbookError(
                "recovery_steps must be a non-empty list of actionable steps"
            )

        # Validate each actionable step has machine-actionable keys
        for step in self.immediate_actions + self.recovery_steps:
            if not isinstance(step, dict):
                raise RunbookError(
                    "each step must be a dict with action_type and parameters"
                )
            if "action_type" not in step:
                raise RunbookError("each step must include action_type")
            # for command steps require 'command' and 'expected_result'
            if step["action_type"] == "command":
                if "command" not in step or "expected_result" not in step:
                    raise RunbookError(
                        "command steps must include 'command' and 'expected_result'"
                    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "incident_type": self.incident_type,
            "summary": self.summary,
            "immediate_actions": self.immediate_actions,
            "escalation_chain": self.escalation_chain,
            "recovery_steps": self.recovery_steps,
            "created_at": self.created_at,
        }


class RunbookManager:
    def __init__(self):
        self._store: Dict[str, Runbook] = {}
        self.audit_log: List[Dict[str, Any]] = []

    def _audit(self, actor: str, action: str, details: Dict[str, Any]) -> None:
        self.audit_log.append(
            {
                "ts": datetime.datetime.utcnow().isoformat() + "Z",
                "actor": actor,
                "action": action,
                "details": details,
            }
        )

    def add_runbook(self, rb: Runbook, actor: str = "system") -> None:
        rb.validate()
        self._store[rb.incident_type] = rb
        self._audit(
            actor, "runbook_added", {"id": rb.id, "incident_type": rb.incident_type}
        )

    def get_runbook(self, incident_type: str) -> Runbook:
        if incident_type not in self._store:
            raise RunbookError("runbook_not_found")
        return self._store[incident_type]

    def execute_runbook(
        self, incident_type: str, actor: str = "system", simulate: bool = True
    ) -> Dict[str, Any]:
        rb = self.get_runbook(incident_type)
        rb.validate()

        execution_record: Dict[str, Any] = {
            "runbook_id": rb.id,
            "incident_type": incident_type,
            "actor": actor,
            "ts": datetime.datetime.utcnow().isoformat() + "Z",
        }
        steps_list: List[Dict[str, Any]] = []

        for step in rb.immediate_actions:
            step_record = {
                "action_type": step.get("action_type"),
                "params": step.get("params", {}),
                "status": "skipped",
            }
            if simulate:
                # simulate execution deterministically: record expected_result
                step_record["status"] = "simulated"
                step_record["expected_result"] = step.get("expected_result")
            else:
                # in a real system, implement the action execution here
                step_record["status"] = "not_implemented"
            steps_list.append(step_record)

        execution_record["steps"] = steps_list

        self._audit(
            actor,
            "runbook_executed",
            {
                "runbook_id": rb.id,
                "incident_type": incident_type,
                "simulate": simulate,
                "execution_record": execution_record,
            },
        )
        return execution_record


@dataclass
class StepResult:
    name: str
    success: bool
    info: dict = field(default_factory=dict)
    ts: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )


class RunbookEngine:
    def __init__(
        self,
        audit_fn: Callable[[str, dict], None],
        sentinel_api=None,
        incident_fn: Optional[Callable[[dict], None]] = None,
    ):
        self.audit = audit_fn
        self.sentinel = sentinel_api
        self.incident_fn = incident_fn or (lambda i: None)
        self.runbooks: Dict[str, List[Callable[[Dict[str, Any]], StepResult]]] = {}

    def register(self, name: str, steps: List[Callable[[Dict[str, Any]], StepResult]]):
        self.runbooks[name] = steps

    def execute(self, name: str, ctx: Dict[str, Any]) -> List[StepResult]:
        steps = self.runbooks.get(name)
        if steps is None:
            raise KeyError(f"runbook not found: {name}")

        results: List[StepResult] = []
        self.audit("runbook_start", {"runbook": name, "ctx": ctx})
        for step in steps:
            step_name = getattr(step, "__name__", str(step))
            try:
                res = step(ctx)
                if not isinstance(res, StepResult):
                    raise TypeError("step must return StepResult")
            except Exception as e:
                # log failure
                res = StepResult(name=step_name, success=False, info={"error": str(e)})
                self.audit(
                    "runbook_step_failure",
                    {"runbook": name, "step": step_name, "error": str(e)},
                )
                # escalate incident
                inc = {"runbook": name, "step": step_name, "error": str(e), "ctx": ctx}
                try:
                    self.incident_fn(inc)
                except Exception:
                    pass
                if self.sentinel and hasattr(self.sentinel, "set_gate"):
                    try:
                        self.sentinel.set_gate(3, f"runbook_failure:{name}:{step_name}")
                    except Exception:
                        pass
                results.append(res)
                break

            # success logging
            self.audit(
                "runbook_step",
                {
                    "runbook": name,
                    "step": res.name,
                    "success": res.success,
                    "info": res.info,
                },
            )
            results.append(res)
            if not res.success:
                # escalate
                inc = {"runbook": name, "step": res.name, "info": res.info}
                try:
                    self.incident_fn(inc)
                except Exception:
                    pass
                if self.sentinel and hasattr(self.sentinel, "set_gate"):
                    try:
                        self.sentinel.set_gate(
                            2, f"runbook_step_warn:{name}:{res.name}"
                        )
                    except Exception:
                        pass
                break

        self.audit(
            "runbook_end", {"runbook": name, "results": [r.__dict__ for r in results]}
        )
        return results
