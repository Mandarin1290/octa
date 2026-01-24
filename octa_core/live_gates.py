from __future__ import annotations

import datetime
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class LiveGateFailure(Exception):
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


@dataclass
class GateResult:
    name: str
    passed: bool
    details: Dict[str, Any] = field(default_factory=dict)


class LiveGates:
    """Evaluate live-readiness gates: risk, execution, data integrity, governance.

    All gates must pass for live trading to be allowed. Any failure blocks capital.
    """

    def __init__(self):
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

    def check_risk(
        self, metrics: Dict[str, float], thresholds: Dict[str, float]
    ) -> GateResult:
        violations: Dict[str, Any] = {}
        for k, thresh in thresholds.items():
            val = metrics.get(k)
            if val is None:
                violations[k] = {"reason": "missing_metric"}
                continue
            if val > thresh:
                violations[k] = {
                    "value": val,
                    "threshold": thresh,
                    "reason": "exceeds_threshold",
                }

        passed = len(violations) == 0
        return GateResult(
            name="risk_metrics", passed=passed, details={"violations": violations}
        )

    def check_execution(self, health: Dict[str, Any]) -> GateResult:
        # health may include latency_ms, failure_rate, connected
        violations: Dict[str, Any] = {}
        if not health.get("connected", True):
            violations["connected"] = {"reason": "disconnected"}
        if health.get("latency_ms", 0) > health.get("latency_threshold_ms", 500):
            violations["latency_ms"] = {
                "value": health.get("latency_ms"),
                "threshold": health.get("latency_threshold_ms"),
            }
        if health.get("failure_rate", 0) > health.get("failure_rate_threshold", 0.01):
            violations["failure_rate"] = {
                "value": health.get("failure_rate"),
                "threshold": health.get("failure_rate_threshold"),
            }

        passed = len(violations) == 0
        return GateResult(
            name="execution_health", passed=passed, details={"violations": violations}
        )

    def check_data_integrity(self, data_checks: Dict[str, Any]) -> GateResult:
        # data_checks: {"source": {"last_update_age_s":..., "max_age_s":...}, ...}
        violations: Dict[str, Any] = {}
        for src, info in data_checks.items():
            age = info.get("last_update_age_s")
            max_age = info.get("max_age_s")
            if age is None or max_age is None:
                violations[src] = {"reason": "missing_fields"}
                continue
            if age > max_age:
                violations[src] = {"age": age, "max_age": max_age, "reason": "stale"}

        passed = len(violations) == 0
        return GateResult(
            name="data_integrity", passed=passed, details={"violations": violations}
        )

    def check_governance(self, clearance: Dict[str, Any]) -> GateResult:
        # clearance: {"approvals": [...], "required_roles": set([...]), "required_count": int}
        approvals = set(clearance.get("approvals", []))
        required_roles = set(clearance.get("required_roles", []))
        required_count = int(clearance.get("required_count", 1))

        details = {
            "approvals": list(approvals),
            "required_roles": list(required_roles),
            "required_count": required_count,
        }

        # count-based check
        if len(approvals) < required_count:
            return GateResult(
                name="governance_clearance",
                passed=False,
                details={**details, "reason": "insufficient_approvals"},
            )

        # roles check
        approved_roles = set(clearance.get("approved_roles", []))
        if required_roles and not required_roles.issubset(approved_roles):
            return GateResult(
                name="governance_clearance",
                passed=False,
                details={
                    **details,
                    "reason": "required_roles_missing",
                    "approved_roles": list(approved_roles),
                },
            )

        return GateResult(name="governance_clearance", passed=True, details=details)

    def evaluate_all(
        self,
        risk_metrics: Dict[str, float],
        risk_thresholds: Dict[str, float],
        execution_health: Dict[str, Any],
        data_checks: Dict[str, Any],
        governance_clearance: Dict[str, Any],
        actor: str = "system",
    ) -> List[GateResult]:
        results = []
        results.append(self.check_risk(risk_metrics, risk_thresholds))
        results.append(self.check_execution(execution_health))
        results.append(self.check_data_integrity(data_checks))
        results.append(self.check_governance(governance_clearance))

        passed_all = all(r.passed for r in results)
        self._audit(
            actor,
            "live_gates_evaluated",
            {"passed_all": passed_all, "results": [r.__dict__ for r in results]},
        )
        return results

    def enforce_live(
        self,
        risk_metrics: Dict[str, float],
        risk_thresholds: Dict[str, float],
        execution_health: Dict[str, Any],
        data_checks: Dict[str, Any],
        governance_clearance: Dict[str, Any],
        actor: str = "system",
    ) -> bool:
        results = self.evaluate_all(
            risk_metrics,
            risk_thresholds,
            execution_health,
            data_checks,
            governance_clearance,
            actor=actor,
        )
        failed = [r for r in results if not r.passed]
        if failed:
            details = {
                "failed_gates": [r.name for r in failed],
                "results": [r.__dict__ for r in results],
            }
            self._audit(actor, "live_gates_blocked", details)
            raise LiveGateFailure(
                "Live-readiness gates failed; capital blocked", details=details
            )

        # all gates passed
        self._audit(
            actor, "live_gates_passed", {"results": [r.__dict__ for r in results]}
        )
        return True
