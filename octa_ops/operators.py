from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class Role(str, Enum):
    VIEW = "VIEW"
    INCIDENT = "INCIDENT"
    EMERGENCY = "EMERGENCY"


@dataclass
class Operator:
    operator_id: str
    role: Role
    key: str  # shared secret used for simple signatures


def _sign(payload: str, key: str) -> str:
    h = hashlib.sha256()
    h.update(key.encode())
    h.update(payload.encode())
    return h.hexdigest()


class OperatorRegistry:
    def __init__(self, audit_fn: Optional[Callable[[str, dict], None]] = None):
        self.operators: Dict[str, Operator] = {}
        self.audit = audit_fn or (lambda e, p: None)

    def register(self, op: Operator):
        self.operators[op.operator_id] = op
        self.audit(
            "operator_registered", {"operator_id": op.operator_id, "role": op.role}
        )

    def get(self, operator_id: str) -> Optional[Operator]:
        return self.operators.get(operator_id)

    def sign(self, operator_id: str, payload: str) -> Optional[str]:
        op = self.get(operator_id)
        if not op:
            return None
        return _sign(payload, op.key)

    def verify(self, operator_id: str, payload: str, signature: str) -> bool:
        op = self.get(operator_id)
        if not op:
            return False
        return _sign(payload, op.key) == signature


class Action:
    def __init__(
        self,
        name: str,
        handler: Callable[[Dict[str, Any]], Any],
        allowed_roles: List[Role],
        dangerous: bool = False,
    ):
        self.name = name
        self.handler = handler
        self.allowed_roles = allowed_roles
        self.dangerous = dangerous


class ActionRegistry:
    def __init__(
        self,
        operator_registry: OperatorRegistry,
        audit_fn: Optional[Callable[[str, dict], None]] = None,
    ):
        self.actions: Dict[str, Action] = {}
        self.operators = operator_registry
        self.audit = audit_fn or (lambda e, p: None)

    def register_action(self, action: Action):
        self.actions[action.name] = action
        self.audit(
            "action_registered",
            {
                "action": action.name,
                "allowed_roles": [r.value for r in action.allowed_roles],
                "dangerous": action.dangerous,
            },
        )

    def execute(
        self,
        operator_id: str,
        action_name: str,
        ctx: Dict[str, Any],
        signature: Optional[str] = None,
        signature2: Optional[str] = None,
    ) -> Dict[str, Any]:
        op = self.operators.get(operator_id)
        if not op:
            self.audit("operator_unknown", {"operator_id": operator_id})
            return {"ok": False, "error": "unknown_operator"}

        action = self.actions.get(action_name)
        if not action:
            return {"ok": False, "error": "unknown_action"}

        # role check
        if op.role not in action.allowed_roles:
            self.audit(
                "permission_denied", {"operator": operator_id, "action": action_name}
            )
            return {"ok": False, "error": "permission_denied"}

        # dangerous actions require dual-control: two signatures from EMERGENCY operators
        if action.dangerous:
            # determine canonical payload for signatures
            payload = (
                ctx.get("payload")
                or f"{action_name}|{operator_id}|{ctx.get('payload_ts', datetime.now(timezone.utc).isoformat())}"
            )
            # verify primary signature
            if not signature or not self.operators.verify(
                operator_id, payload, signature
            ):
                self.audit(
                    "signature_invalid",
                    {"operator": operator_id, "action": action_name},
                )
                return {"ok": False, "error": "invalid_signature"}

            # second signature required
            if not signature2:
                self.audit("signature_missing", {"action": action_name})
                return {"ok": False, "error": "second_signature_required"}

            # parse second operator id from ctx
            second_id = ctx.get("second_operator")
            if not second_id:
                return {"ok": False, "error": "second_operator_missing"}
            if not self.operators.verify(second_id, payload, signature2):
                self.audit(
                    "second_signature_invalid",
                    {"second_operator": second_id, "action": action_name},
                )
                return {"ok": False, "error": "invalid_second_signature"}

        # execute handler
        try:
            res = action.handler(ctx)
            self.audit(
                "action_executed",
                {"operator": operator_id, "action": action_name, "result": res},
            )
            return {"ok": True, "result": res}
        except Exception as e:
            self.audit(
                "action_failed",
                {"operator": operator_id, "action": action_name, "error": str(e)},
            )
            return {"ok": False, "error": "handler_error", "detail": str(e)}
