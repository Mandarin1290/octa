from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from octa_core.kill_switch import kill_switch
from octa_core.security.audit import AuditLog


@dataclass(frozen=True)
class SafeStopConfig:
    mode: str = "SAFE"  # SAFE|IMMEDIATE
    flatten_positions: bool = False
    hedge_to_neutral: bool = False


class ExecutionFacade:
    """Minimal execution facade for safe-stop.

    This is intentionally interface-based so we don't have to change existing execution modules.
    """

    def cancel_open_orders(self) -> None:  # pragma: no cover
        raise NotImplementedError

    def flatten_positions(self) -> None:  # pragma: no cover
        raise NotImplementedError

    def hedge_to_neutral(self) -> None:  # pragma: no cover
        raise NotImplementedError


def safe_stop(
    *,
    mode: str,
    exec_api: Optional[ExecutionFacade],
    audit_path: str,
    flatten_positions: bool = False,
    hedge_to_neutral: bool = False,
) -> Dict[str, Any]:
    """Fail-closed SAFE STOP.

    1) Engage kill switch to block new activity in wrappers.
    2) Cancel open orders (best-effort).
    3) Optionally flatten or hedge (only when configured by caller).
    4) Always emit audit event.
    """

    alog = AuditLog(path=audit_path)
    kill_switch.engage(actor="control_plane", reason=f"safe_stop:{mode}", automated=True)

    details: Dict[str, Any] = {"mode": mode, "cancelled": False, "flattened": False, "hedged": False}

    try:
        if exec_api is not None:
            exec_api.cancel_open_orders()
            details["cancelled"] = True
    except Exception as e:
        details["cancel_error"] = str(e)

    if mode.upper() == "SAFE":
        # SAFE mode may optionally include flatten/hedge in the calling config.
        try:
            if exec_api is not None and bool(flatten_positions):
                exec_api.flatten_positions()
                details["flattened"] = True
        except Exception as e:
            details["flatten_error"] = str(e)

        try:
            if exec_api is not None and bool(hedge_to_neutral):
                exec_api.hedge_to_neutral()
                details["hedged"] = True
        except Exception as e:
            details["hedge_error"] = str(e)

    alog.append(event_type="control_plane.safe_stop", payload=details)
    return details


__all__ = ["SafeStopConfig", "ExecutionFacade", "safe_stop"]
