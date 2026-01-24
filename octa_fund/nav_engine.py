from decimal import ROUND_HALF_EVEN, Decimal, getcontext
from typing import Any, Dict

# Set Decimal context for deterministic behaviour
getcontext().prec = 28


class NAVEngine:
    """Deterministic NAV engine.

    Methods:
      - `compute_nav(share_classes, period='daily'|'month')` returns per-class NAV and reconciliation.
      - deterministic rounding: currency rounded to 2 decimal places using ROUND_HALF_EVEN.
    """

    def __init__(self, audit_fn=None):
        self.audit_fn = audit_fn or (lambda e, p: None)

    def _to_decimal(self, v) -> Decimal:
        return Decimal(str(v))

    def _round_currency(self, x: Decimal) -> Decimal:
        return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)

    def _round_nav_per_share(self, x: Decimal) -> Decimal:
        return x.quantize(Decimal("0.000001"), rounding=ROUND_HALF_EVEN)

    def compute_nav(
        self, share_classes: Dict[str, Any], period: str = "daily"
    ) -> Dict[str, Dict[str, Any]]:
        """Compute NAV for each share class.

        `share_classes` may be mapping of class_id -> ShareClass instance or dict-like with keys `cash_balance`, `assets`, `shares_outstanding`.
        Returns mapping class_id -> { total (Decimal), nav_per_share (Decimal)} with deterministic rounding.
        Also returns a reconciliation report via audit event.
        """
        report = {}
        recon = {}
        # process classes in deterministic order
        for cid in sorted(share_classes.keys()):
            sc = share_classes[cid]
            # support both ShareClass object and dict
            if hasattr(sc, "total_value"):
                total = Decimal(str(sc.total_value()))
                shares = Decimal(str(sc.shares_outstanding))
            else:
                cash = Decimal(str(sc.get("cash_balance", 0.0)))
                assets_sum = Decimal("0.0")
                for aid in sorted(sc.get("assets", {}).keys()):
                    assets_sum += Decimal(str(sc.get("assets", {})[aid]))
                total = cash + assets_sum
                shares = Decimal(str(sc.get("shares_outstanding", 0.0)))

            total_r = self._round_currency(total)
            nav_per_share = Decimal("0.0")
            if shares > 0:
                nav_per_share = total / shares
                nav_per_share = self._round_nav_per_share(nav_per_share)

            report[cid] = {"total": total_r, "nav_per_share": nav_per_share}
            # reconciliation entry
            recon[cid] = {
                "computed_total": str(total_r),
                "shares_outstanding": str(shares),
            }

        # Emit audit event with reconciliation
        self.audit_fn("nav.reconciliation", recon)
        return report
