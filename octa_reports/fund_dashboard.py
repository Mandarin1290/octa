from decimal import Decimal
from typing import Any, Dict, List, Optional


class FundDashboard:
    """Deterministic fund dashboard computed from investor ledgers and NAV prices.

    - `investors`: list of `InvestorAccount` instances
    - `nav_prices`: mapping share_class -> Decimal price
    """

    def __init__(self, investors: List[Any], nav_prices: Dict[str, Decimal]):
        self.investors = investors
        self.nav_prices = {k: Decimal(v) for k, v in nav_prices.items()}

    def nav_per_class(self) -> Dict[str, Decimal]:
        return dict(self.nav_prices)

    def investor_balances(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for inv in self.investors:
            cash = Decimal(inv.balance)
            shares = {k: Decimal(v) for k, v in inv.shares.items()}
            # compute market value of shares
            mv = Decimal("0")
            for sc, qty in shares.items():
                price = self.nav_prices.get(sc, Decimal("0"))
                mv += Decimal(qty) * Decimal(price)
            total = (cash + mv).quantize(Decimal("0.00000001"))
            out[inv.investor_id] = {
                "cash": cash.quantize(Decimal("0.00000001")),
                "shares": shares,
                "market_value": mv.quantize(Decimal("0.00000001")),
                "total": total,
            }
        return out

    def fund_aum(self) -> Decimal:
        s = Decimal("0")
        invs = self.investor_balances()
        for v in invs.values():
            s += Decimal(v["total"])
        return s.quantize(Decimal("0.00000001"))

    def fee_accruals(self) -> Decimal:
        """Scan investor ledgers for fee-like entries and sum them deterministically.

        Heuristic: an entry is considered a fee if its event name contains 'fee' or
        its details memo contains 'fee'. This keeps the dashboard deterministic
        and reconcilable with ledger entries.
        """
        total = Decimal("0")
        for inv in self.investors:
            for e in inv.get_history():
                ev = e.get("event", "").lower()
                memo = (e.get("details", {}) or {}).get("memo", "")
                if "fee" in ev or "fee" in str(memo).lower():
                    total += Decimal(e.get("amount"))
        # fees are typically outflows (negative amounts); return positive accrual
        return (
            (-total).quantize(Decimal("0.00000001"))
            if total < 0
            else Decimal("0").quantize(Decimal("0.00000001"))
        )

    def liquidity_and_gate_status(
        self, gate_manager: Optional[Any], available_liquid: Decimal
    ) -> Dict[str, Any]:
        aum = self.fund_aum()
        allowed = gate_manager.allowed_amount(aum) if gate_manager is not None else None
        return {
            "aum": aum,
            "available_liquid": Decimal(available_liquid).quantize(
                Decimal("0.00000001")
            ),
            "gate_allowed": (
                Decimal(allowed).quantize(Decimal("0.00000001"))
                if allowed is not None
                else None
            ),
        }
