from __future__ import annotations

import collections
import datetime
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Set


@dataclass
class DefenseAlert:
    ts: str
    severity: str
    actor: str
    action: str
    reason: str
    details: Dict[str, Any]


class DefenseSystem:
    """Simple, testable defense primitives used by the red-team simulator.

    - Immutable baseline privileges and parameters are provided at init.
    - Parameter ranges enforce numeric bounds.
    - Rate-limiter defends against timing attacks.
    """

    def __init__(
        self,
        baseline_privileges: Dict[str, List[str]],
        allowed_ranges: Dict[str, tuple],
        rate_limit_per_minute: int = 60,
    ):
        self.baseline_privileges: Dict[str, Set[str]] = {
            u: set(r) for u, r in baseline_privileges.items()
        }
        self.allowed_ranges = allowed_ranges
        self.alerts: List[DefenseAlert] = []
        self.rate_limit = rate_limit_per_minute
        self._recent_actions: Dict[str, Deque[float]] = collections.defaultdict(
            collections.deque
        )

    def _now_ts(self) -> str:
        return datetime.datetime.now(datetime.timezone.utc).isoformat()

    def _record_alert(
        self,
        severity: str,
        actor: str,
        action: str,
        reason: str,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.alerts.append(
            DefenseAlert(
                ts=self._now_ts(),
                severity=severity,
                actor=actor,
                action=action,
                reason=reason,
                details=details or {},
            )
        )

    def attempt_privilege_change(
        self,
        actor: str,
        target_user: str,
        new_roles: List[str],
        approvals: Optional[List[str]] = None,
    ) -> bool:
        # No single actor may unilaterally escalate another account beyond baseline roles
        approvals_set: Set[str] = set(approvals or [])
        baseline = self.baseline_privileges.get(target_user, set())
        requested = set(new_roles)

        # if requested subset of baseline -> harmless
        if requested.issubset(baseline):
            return True

        # require at least two approvals from distinct principals
        if len(approvals_set) < 2:
            self._record_alert(
                "high",
                actor,
                "privilege_change",
                "insufficient_approvals",
                {"requested": list(requested), "baseline": list(baseline)},
            )
            return False

        # no single actor absolute control: cannot self-approve
        if actor in approvals_set and len(approvals_set) == 1:
            self._record_alert(
                "high",
                actor,
                "privilege_change",
                "self_approval_disallowed",
                {"approvals": list(approvals_set)},
            )
            return False

        # Accept with approvals but record
        self._record_alert(
            "info",
            actor,
            "privilege_change",
            "accepted_with_approvals",
            {"approvals": list(approvals_set), "requested": list(requested)},
        )
        return True

    def attempt_param_change(
        self, actor: str, param: str, value: Any, approvals: Optional[List[str]] = None
    ) -> bool:
        # Numeric bounds enforcement
        if param in self.allowed_ranges:
            min_v, max_v = self.allowed_ranges[param]
            try:
                v = float(value)
            except Exception:
                self._record_alert(
                    "high",
                    actor,
                    "param_change",
                    "non_numeric_value",
                    {"param": param, "value": value},
                )
                return False

            if (min_v is not None and v < min_v) or (max_v is not None and v > max_v):
                self._record_alert(
                    "high",
                    actor,
                    "param_change",
                    "boundary_violation",
                    {"param": param, "value": v, "min": min_v, "max": max_v},
                )
                return False

        # if not in allowed ranges, require approvals
        if param not in self.allowed_ranges and not approvals:
            self._record_alert(
                "medium",
                actor,
                "param_change",
                "requires_approval",
                {"param": param, "value": value},
            )
            return False

        self._record_alert(
            "info", actor, "param_change", "accepted", {"param": param, "value": value}
        )
        return True

    def attempt_action(self, actor: str, action_id: str) -> bool:
        # Rate limit per actor to detect timing attacks
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        dq = self._recent_actions[actor]
        dq.append(now)
        # expire items older than 60 seconds
        while dq and now - dq[0] > 60:
            dq.popleft()

        if len(dq) > self.rate_limit:
            self._record_alert(
                "high",
                actor,
                "high_frequency_actions",
                "rate_limit_exceeded",
                {"count": len(dq), "limit": self.rate_limit},
            )
            return False

        # action permitted
        self._record_alert(
            "debug", actor, "action_executed", "ok", {"action_id": action_id}
        )
        return True


class RedTeamSimulator:
    """Simulate internal attacks against a DefenseSystem.

    The simulator tries a set of realistic actions; defenses should detect and block without prior per-attack knowledge.
    """

    def __init__(self, defense: DefenseSystem):
        self.defense = defense

    def privilege_misuse(
        self, actor: str, target_user: str, attempt_roles: List[str]
    ) -> bool:
        # actor attempts to escalate privileges on target_user without proper approvals
        return self.defense.attempt_privilege_change(
            actor=actor,
            target_user=target_user,
            new_roles=attempt_roles,
            approvals=None,
        )

    def parameter_manipulation(self, actor: str, param: str, value: Any) -> bool:
        # actor attempts to change configuration param
        return self.defense.attempt_param_change(
            actor=actor, param=param, value=value, approvals=None
        )

    def timing_attack(
        self, actor: str, burst_count: int, action_id_prefix: str = "atk"
    ) -> bool:
        # send burst_count actions as fast as possible
        result = True
        for i in range(burst_count):
            ok = self.defense.attempt_action(actor, f"{action_id_prefix}-{i}")
            result = result and ok
        return result
