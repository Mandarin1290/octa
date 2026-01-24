from datetime import datetime
from typing import Any, Callable, Dict, Optional


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


class FeeBookingEngine:
    """Books crystallized fees as payables (liabilities) prior to settlement.

    - `book_crystallized_fees(series, booking_date, period)` computes fees per class (using class.total_value() and high_water_mark and performance_fee), creates payables and emits audit events.
    - `settle_payable(class_id, series)` pays the payable: deducts cash from the class, updates HWM to post‑fee total, and emits audit event.
    - `reverse_booking(class_id, reason)` reverses a booking (removes payable) and emits an audit event; reversals are auditable.
    """

    def __init__(self, audit_fn: Optional[Callable] = None):
        self.audit_fn = audit_fn or (lambda e, p: None)
        # payables: class_id -> {amount, booked_at, period, paid:bool, paid_at}
        self._payables: Dict[str, Dict[str, Any]] = {}
        self._last_booked_total: Dict[str, float] = {}

    def book_crystallized_fees(
        self, series, booking_date: str | None = None, period: str = "monthly"
    ) -> Dict[str, float]:
        booking_date = booking_date or _now_iso()
        results: Dict[str, float] = {}
        for cid in sorted(series.classes.keys()):
            sc = series.classes[cid]
            perf_rate = float(getattr(sc, "performance_fee", 0.0))
            if perf_rate <= 0.0:
                continue
            current_total = float(sc.total_value())
            prev_hwm = float(getattr(sc, "high_water_mark", 0.0))
            if current_total <= prev_hwm:
                continue
            # prevent double booking on same total
            last = self._last_booked_total.get(cid)
            if last is not None and abs(last - current_total) < 1e-12:
                continue

            realized_gain = current_total - prev_hwm
            fee = realized_gain * perf_rate
            payable = {
                "class_id": cid,
                "amount": float(fee),
                "booked_at": booking_date,
                "period": period,
                "paid": False,
                "paid_at": None,
            }
            self._payables[cid] = payable
            self._last_booked_total[cid] = current_total
            results[cid] = float(fee)
            self.audit_fn(
                "fee_booking.booked",
                {
                    "fund_id": series.fund_id,
                    "class_id": cid,
                    "amount": float(fee),
                    "booked_at": booking_date,
                    "period": period,
                },
            )

        return results

    def get_payables(self) -> Dict[str, Dict[str, Any]]:
        return {k: dict(v) for k, v in self._payables.items()}

    def settle_payable(self, class_id: str, series) -> float:
        pay = self._payables.get(class_id)
        if not pay or pay.get("paid"):
            raise RuntimeError("No unpaid payable for class")
        amount = float(pay["amount"])
        sc = series.classes[class_id]
        # deduct cash
        sc.cash_balance -= amount
        pay["paid"] = True
        pay["paid_at"] = _now_iso()
        # update HWM to post-fee total
        post_total = float(sc.total_value())
        sc.high_water_mark = post_total
        self.audit_fn(
            "fee_booking.settled",
            {"class_id": class_id, "amount": amount, "paid_at": pay["paid_at"]},
        )
        return amount

    def reverse_booking(self, class_id: str, reason: str = "reversal") -> bool:
        pay = self._payables.get(class_id)
        if not pay:
            return False
        if pay.get("paid"):
            raise RuntimeError("Cannot reverse a paid booking")
        del self._payables[class_id]
        # clear last booked total so future bookings possible
        if class_id in self._last_booked_total:
            del self._last_booked_total[class_id]
        self.audit_fn(
            "fee_booking.reversed",
            {"class_id": class_id, "reason": reason, "timestamp": _now_iso()},
        )
        return True
