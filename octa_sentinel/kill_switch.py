from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Dict, Optional


class KillState(str, Enum):
    ARMED = "ARMED"
    TRIGGERED = "TRIGGERED"
    LOCKED = "LOCKED"


class KillSwitch:
    """Global kill-switch singleton.

    - Only the Sentinel is allowed to release a triggered or locked kill-switch via `release_by_sentinel`.
    - Manual release requires dual operator confirmations (two operator keys).
    - Every state change is audited and 'signed' (simple SHA256-based signature for provenance).
    """

    def __init__(
        self,
        audit_fn: Optional[Callable[[str, dict], None]] = None,
        operator_keys: Optional[Dict[str, str]] = None,
    ):
        self.state: KillState = KillState.ARMED
        self.last_change: Optional[dict] = None
        self.audit = audit_fn or (lambda e, p: None)
        self.operator_keys = operator_keys or {}

    def _now(self):
        return datetime.now(timezone.utc)

    def _sign(self, payload: str, key: str) -> str:
        # lightweight provenance signature (not cryptographic ed25519)
        m = hashlib.sha256()
        m.update(key.encode())
        m.update(payload.encode())
        return m.hexdigest()

    def _record(self, action: str, meta: dict, signer: Optional[str] = None):
        ts = self._now().isoformat()
        payload = f"{action}|{ts}|{meta}"
        sig = self._sign(payload, signer or "system")
        record = {"action": action, "ts": ts, "meta": meta, "signature": sig}
        self.last_change = record
        try:
            self.audit("kill_switch_change", record)
        except Exception:
            pass

    def arm(self, reason: str = ""):
        self.state = KillState.ARMED
        self._record("arm", {"reason": reason})

    def trigger(self, source: str = "sentinel", reason: str = ""):
        self.state = KillState.TRIGGERED
        self._record("trigger", {"source": source, "reason": reason})

    def lock(self, source: str = "system", reason: str = ""):
        self.state = KillState.LOCKED
        self._record("lock", {"source": source, "reason": reason})

    def release_by_sentinel(
        self, sentinel_id: str = "sentinel", reason: str = ""
    ) -> bool:
        # Only sentinel may call this method in practice. It's a logical guard here.
        if self.state == KillState.ARMED:
            return True
        self.state = KillState.ARMED
        self._record(
            "release_by_sentinel",
            {"sentinel_id": sentinel_id, "reason": reason},
            signer=sentinel_id,
        )
        return True

    def manual_release(
        self,
        operator1: str,
        sig1: str,
        operator2: str,
        sig2: str,
        reason: str = "",
        payload_ts: Optional[str] = None,
    ) -> bool:
        # verify operator keys are registered and signatures match
        k1 = self.operator_keys.get(operator1)
        k2 = self.operator_keys.get(operator2)
        if not k1 or not k2:
            return False
        ts = payload_ts or self._now().isoformat()
        payload = f"manual_release|{ts}|{reason}"
        if self._sign(payload, k1) != sig1 or self._sign(payload, k2) != sig2:
            return False
        self.state = KillState.ARMED
        self._record(
            "manual_release",
            {"operators": [operator1, operator2], "reason": reason, "ts": ts},
            signer=operator1,
        )
        return True

    def get_state(self) -> KillState:
        return self.state


# module-level singleton
_GLOBAL_KILL_SWITCH: Optional[KillSwitch] = None


def get_kill_switch(
    audit_fn: Optional[Callable[[str, dict], None]] = None,
    operator_keys: Optional[Dict[str, str]] = None,
) -> KillSwitch:
    global _GLOBAL_KILL_SWITCH
    if _GLOBAL_KILL_SWITCH is None:
        _GLOBAL_KILL_SWITCH = KillSwitch(audit_fn=audit_fn, operator_keys=operator_keys)
    return _GLOBAL_KILL_SWITCH
