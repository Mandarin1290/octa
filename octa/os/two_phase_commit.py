from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from octa.execution.risk_engine import RiskEngine

from .state_store import OSStateStore
from .utils import utc_now_iso


@dataclass(frozen=True)
class TwoPhaseCommitResult:
    order_id: str
    approved: bool
    sent: bool
    reason: str
    intent_path: str
    approval_path: str
    broker_result: dict[str, Any] | None


class TwoPhaseCommitEngine:
    def __init__(self, state_store: OSStateStore, risk_engine: RiskEngine) -> None:
        self._state = state_store
        self._risk = risk_engine

    def phase1_intent(self, order_id: str, intent: dict[str, Any]) -> str:
        path = self._state.write_intent(order_id, {"ts_utc": utc_now_iso(), **intent})
        return str(path)

    def phase2_approve(
        self, order_id: str, nav: float, scaling_level: int, current_gross: float
    ) -> tuple[str, bool, dict[str, Any]]:
        decision = self._risk.decide_ml(
            nav=nav, scaling_level=scaling_level, current_gross_exposure_pct=current_gross
        )
        approval = {
            "ts_utc": utc_now_iso(),
            "order_id": order_id,
            "approved": bool(decision.allow),
            "reason": str(decision.reason),
            "risk_snapshot": dict(decision.risk_snapshot),
            "final_size": float(decision.final_size),
        }
        path = self._state.write_approval(order_id, approval)
        return str(path), bool(decision.allow), approval

    def commit(
        self,
        *,
        order_id: str,
        sensors_ok: bool,
        live_commit_ok: bool,
        send_fn: Any,
    ) -> tuple[bool, str, dict[str, Any] | None]:
        intent = self._state.read_intent(order_id)
        approval = self._state.read_approval(order_id)

        if not intent or not approval:
            return False, "missing_intent_or_approval", None
        if str(intent.get("order_id", "")) != str(approval.get("order_id", "")):
            return False, "intent_approval_mismatch", None
        if not bool(approval.get("approved", False)):
            return False, "risk_rejected", None
        if not sensors_ok:
            return False, "sensor_flip_abort", None
        if not live_commit_ok:
            return False, "live_not_armed_at_commit", None

        order = {
            "order_id": order_id,
            "instrument": str(intent.get("symbol", "")),
            "qty": float(intent.get("qty", 0.0)),
            "side": str(intent.get("side", "BUY")),
            "order_type": "MKT",
        }
        result = send_fn(order)
        return True, "sent", result
