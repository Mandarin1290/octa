from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class SoftCloseRecord:
    timestamp: str
    active: bool
    reason: str
    details: Dict[str, Any]


class SoftCloseEngine:
    """Soft Close (Growth Brake)

    - Prevents external inflows when active.
    - Reversible via `lift()`.
    - Attach to an `AUMState` to automatically block external inflows.
    - Triggered by capacity utilization, slippage deterioration, or correlation crowding.
    """

    def __init__(
        self,
        thresholds: Dict[str, float] | None = None,
        audit_fn: Callable[[str, Dict[str, Any]], None] | None = None,
    ):
        t = thresholds or {
            "capacity_utilization": 0.9,
            "slippage_delta": 0.5,
            "correlation_index": 0.8,
        }
        self.cap_th = float(t.get("capacity_utilization", 0.9))
        self.slip_th = float(t.get("slippage_delta", 0.5))
        self.corr_th = float(t.get("correlation_index", 0.8))
        self.audit_fn: Callable[[str, Dict[str, Any]], None] = audit_fn or (
            lambda e, p: None
        )
        self.active = False
        self._history: List[SoftCloseRecord] = []
        self._patched = False
        from typing import Any as _Any

        self._orig_inflow: Callable[..., _Any] = lambda *a, **k: False

    def _record(self, active: bool, reason: str, details: Dict[str, Any]):
        rec = SoftCloseRecord(
            timestamp=_now_iso(), active=active, reason=reason, details=details
        )
        self._history.append(rec)
        self.audit_fn(
            "capital.soft_close." + ("activated" if active else "lifted"), asdict(rec)
        )

    def check_and_update(
        self,
        capacity_utilization: float = 0.0,
        slippage_delta: float = 0.0,
        correlation_index: float = 0.0,
    ):
        reasons = []
        if capacity_utilization > self.cap_th:
            reasons.append("capacity_utilization")
        if slippage_delta > self.slip_th:
            reasons.append("slippage_delta")
        if correlation_index > self.corr_th:
            reasons.append("correlation_index")

        if reasons and not self.active:
            self.active = True
            self._record(
                True,
                ",".join(reasons),
                {
                    "capacity_utilization": capacity_utilization,
                    "slippage_delta": slippage_delta,
                    "correlation_index": correlation_index,
                },
            )
        return self.active

    def lift(self, reason: str = "manual_lift"):
        if self.active:
            self.active = False
            self._record(False, reason, {})

    def attach(self, aum_state):
        # Monkey-patch inflow to block external inflows when soft close is active
        if self._patched:
            return

        self._orig_inflow = aum_state.inflow

        def _inflow_wrapper(
            amount: float, source: str = "external", reason: str = "inflow"
        ):
            if self.active and source == "external":
                # block external inflows but allow internal compounding
                self.audit_fn(
                    "capital.inflow.blocked",
                    {
                        "timestamp": _now_iso(),
                        "amount": float(amount),
                        "reason": reason,
                    },
                )
                return False
            return self._orig_inflow(amount, source=source, reason=reason)

        aum_state.inflow = _inflow_wrapper
        self._patched = True

    def history(self) -> List[SoftCloseRecord]:
        return list(self._history)
