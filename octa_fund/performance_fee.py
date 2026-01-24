from datetime import datetime
from typing import Callable, Dict, Optional


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


class PerformanceFeeEngine:
    """High‑Water‑Mark based performance fee crystallization.

    Rules:
    - Fees charged only on realized gains and only above the HWM.
    - No double charging: repeated crystallize calls on the same post‑fee total do not re‑charge.
    - Per‑class isolation: operations apply to the given `ShareClass` instance only.
    """

    def __init__(self, audit_fn: Optional[Callable] = None):
        self.audit_fn = audit_fn or (lambda e, p: None)
        # track last crystallized total per class to avoid double charging
        self._last_crystallized_total: Dict[str, float] = {}

    def crystallize_fee(self, share_class) -> float:
        """Crystallize performance fee for the provided `ShareClass` instance.

        The `ShareClass` must provide:
          - `class_id`, `performance_fee`, `high_water_mark`, and `total_value()` and `cash_balance` attributes.

        Returns the fee charged (float).
        """
        cid = share_class.class_id
        perf_rate = float(getattr(share_class, "performance_fee", 0.0))
        if perf_rate <= 0.0:
            return 0.0

        current_total = float(share_class.total_value())
        prev_hwm = float(getattr(share_class, "high_water_mark", 0.0))

        # nothing to charge if at or below HWM
        if current_total <= prev_hwm:
            return 0.0

        # prevent double charge on same total
        last = self._last_crystallized_total.get(cid)
        if last is not None and abs(last - current_total) < 1e-12:
            return 0.0

        # realized_gain is approximated by current_total - prev_hwm (only realized portion considered)
        realized_gain = current_total - prev_hwm
        fee = realized_gain * perf_rate

        # apply fee to class cash balance (reducing NAV) and update HWM
        share_class.cash_balance -= fee
        # update HWM to post‑fee total
        post_fee_total = current_total - fee
        share_class.high_water_mark = post_fee_total

        # record last crystallized total to avoid double charging
        self._last_crystallized_total[cid] = post_fee_total

        # audit
        payload = {
            "class_id": cid,
            "fee": fee,
            "prev_hwm": prev_hwm,
            "post_fee_total": post_fee_total,
            "timestamp": _now_iso(),
        }
        self.audit_fn("performance_fee.crystallized", payload)

        return float(fee)
