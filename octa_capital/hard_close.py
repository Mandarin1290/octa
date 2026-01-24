from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Set


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class HardCloseRecord:
    timestamp: str
    active: bool
    reason: str
    details: Dict[str, Any]


class HardCloseEngine:
    """Hard Close (Capacity Protection)

    - When active: absolutely rejects all external inflows.
    - Manual override requires committee approval (n approvals).
    - Triggers: absolute capacity reached, persistent alpha decay, regulatory/liquidity flags.
    """

    def __init__(
        self,
        absolute_cap: float,
        required_approvals: int = 2,
        audit_fn: Callable[[str, Dict[str, Any]], None] | None = None,
    ):
        self.absolute_cap = float(absolute_cap)
        self.required_approvals = int(required_approvals)
        self.audit_fn: Callable[[str, Dict[str, Any]], None] = audit_fn or (
            lambda e, p: None
        )
        self.active = False
        self._history: List[HardCloseRecord] = []
        self._patched = False
        from typing import Any as _Any

        self._orig_inflow: Callable[..., _Any] = lambda *a, **k: False
        self._approvals: Set[str] = set()

    def _record(self, active: bool, reason: str, details: Dict[str, Any]):
        rec = HardCloseRecord(
            timestamp=_now_iso(), active=active, reason=reason, details=details
        )
        self._history.append(rec)
        self.audit_fn(
            "capital.hard_close." + ("activated" if active else "overridden"),
            asdict(rec),
        )

    def check_and_update(
        self,
        aum_total: float = 0.0,
        persistent_alpha: bool = False,
        regulatory_flag: bool = False,
        liquidity_flag: bool = False,
    ):
        # Activation conditions
        reasons = []
        if aum_total >= self.absolute_cap:
            reasons.append("absolute_capacity")
        if persistent_alpha:
            reasons.append("persistent_alpha")
        if regulatory_flag:
            reasons.append("regulatory")
        if liquidity_flag:
            reasons.append("liquidity")

        if reasons and not self.active:
            self.active = True
            self._record(True, ",".join(reasons), {"aum_total": float(aum_total)})
        return self.active

    def attach(self, aum_state):
        # subscribe to snapshots and auto-activate when absolute cap reached
        def cb(snap):
            if snap.computed_total >= self.absolute_cap and not self.active:
                self.check_and_update(aum_total=snap.computed_total)

        aum_state.subscribe(cb)

        # patch inflow to reject when active
        if not self._patched:
            self._orig_inflow = aum_state.inflow

            def _inflow_wrapper(
                amount: float, source: str = "external", reason: str = "inflow"
            ):
                if self.active and source == "external":
                    self.audit_fn(
                        "capital.inflow.rejected",
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

    def request_approval(self, approver_id: str) -> None:
        self._approvals.add(approver_id)
        self.audit_fn(
            "capital.hard_close.approval",
            {
                "timestamp": _now_iso(),
                "approver": approver_id,
                "count": len(self._approvals),
            },
        )

    def lift_if_approved(self) -> bool:
        if len(self._approvals) >= self.required_approvals and self.active:
            self.active = False
            details = {"approvals": list(self._approvals)}
            self._record(False, "committee_approved", details)
            self._approvals.clear()
            return True
        return False

    def history(self) -> List[HardCloseRecord]:
        return list(self._history)
