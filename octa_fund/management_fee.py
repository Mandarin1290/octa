from typing import Dict


class ManagementFeeEngine:
    """Accrues management fees daily for share classes.

    Fees are computed as: fee = total_value * (annual_rate) * (days/365).
    Fees are deducted from the class cash balance (reducing NAV), and audit events are emitted.
    """

    def __init__(self, audit_fn=None):
        self.audit_fn = audit_fn or (lambda e, p: None)

    def accrue_daily(self, series) -> Dict[str, float]:
        """Accrue one day of management fees for all share classes in `series`.

        `series` is expected to be a `ShareClassSeries` with `.classes` mapping.
        Returns mapping class_id -> fee_amount (float).
        """
        results: Dict[str, float] = {}
        for cid in sorted(series.classes.keys()):
            sc = series.classes[cid]
            fee = sc.apply_management_fee(period_days=1)
            results[cid] = float(fee)

        # emit a daily summary audit event
        self.audit_fn("management_fee.daily_accrual", {"fees": results})
        return results
