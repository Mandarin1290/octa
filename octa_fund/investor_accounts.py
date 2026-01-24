import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import Any, Callable, Dict, List

getcontext().prec = 12

AuditFn = Callable[[str, Dict[str, Any]], None]


def _noop_audit(event: str, payload: Dict[str, Any]) -> None:
    return None


@dataclass
class LedgerEntry:
    id: str
    timestamp: str
    event: str
    amount: Decimal  # cash effect: positive inflow, negative outflow
    balance_after: Decimal
    details: Dict[str, Any] = field(default_factory=dict)


class InvestorAccount:
    """Ledger-first investor capital account.

    Hard rules enforced:
    - Each investor has an independent account (separate instance).
    - All changes are recorded as ledger entries; balance and shares are
      derived/checked from the ledger (reconciliation supported).
    - Capital flows do not affect strategy logic here — this is pure
      accounting for investor-level state.
    """

    def __init__(
        self,
        investor_id: str,
        owner: str,
        audit_fn: AuditFn = _noop_audit,
    ):
        self.investor_id = investor_id
        self.owner = owner
        self._balance: Decimal = Decimal("0")
        self._shares: Dict[str, Decimal] = {}
        self._ledger: List[LedgerEntry] = []
        self.audit_fn = audit_fn

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _append(
        self, event: str, amount: Decimal, details: Dict[str, Any]
    ) -> LedgerEntry:
        self._balance = (self._balance + amount).quantize(Decimal("0.00000001"))
        entry = LedgerEntry(
            id=str(uuid.uuid4()),
            timestamp=self._now_iso(),
            event=event,
            amount=amount,
            balance_after=self._balance,
            details=details,
        )
        self._ledger.append(entry)
        try:
            self.audit_fn(event, {"investor_id": self.investor_id, "entry": details})
        except Exception:
            pass
        return entry

    @property
    def balance(self) -> Decimal:
        return self._balance

    @property
    def shares(self) -> Dict[str, Decimal]:
        return dict(self._shares)

    def deposit(self, amount: Decimal, memo: str = "deposit") -> LedgerEntry:
        amt = Decimal(amount)
        if amt <= 0:
            raise ValueError("deposit amount must be positive")
        return self._append("cash.deposit", amt, {"memo": memo})

    def withdraw(self, amount: Decimal, memo: str = "withdrawal") -> LedgerEntry:
        amt = Decimal(amount)
        if amt <= 0:
            raise ValueError("withdraw amount must be positive")
        if self._balance - amt < Decimal("0"):
            raise ValueError("insufficient funds")
        return self._append("cash.withdrawal", -amt, {"memo": memo})

    def buy_shares(
        self, share_class: str, num_shares: Decimal, price_per_share: Decimal
    ) -> LedgerEntry:
        num = Decimal(num_shares)
        price = Decimal(price_per_share)
        if num <= 0 or price <= 0:
            raise ValueError("shares and price must be positive")
        total = (num * price).quantize(Decimal("0.00000001"))
        if self._balance - total < Decimal("0"):
            raise ValueError("insufficient funds to buy shares")
        # cash outflow
        details = {"share_class": share_class, "shares_delta": num, "price": str(price)}
        entry = self._append("shares.purchase", -total, details)
        # update shares ledger-independent state for convenience
        self._shares[share_class] = self._shares.get(share_class, Decimal("0")) + num
        return entry

    def sell_shares(
        self, share_class: str, num_shares: Decimal, price_per_share: Decimal
    ) -> LedgerEntry:
        num = Decimal(num_shares)
        price = Decimal(price_per_share)
        if num <= 0 or price <= 0:
            raise ValueError("shares and price must be positive")
        existing = self._shares.get(share_class, Decimal("0"))
        if existing - num < Decimal("0"):
            raise ValueError("insufficient shares to sell")
        total = (num * price).quantize(Decimal("0.00000001"))
        details = {
            "share_class": share_class,
            "shares_delta": -num,
            "price": str(price),
        }
        entry = self._append("shares.sale", total, details)
        self._shares[share_class] = existing - num
        return entry

    def get_history(self) -> List[Dict[str, Any]]:
        return [
            {
                "id": e.id,
                "timestamp": e.timestamp,
                "event": e.event,
                "amount": str(e.amount),
                "balance_after": str(e.balance_after),
                "details": e.details,
            }
            for e in self._ledger
        ]

    def reconcile(self) -> bool:
        """Verify ledger-derived balances and shares match stored state.

        Returns True when balances and shares match; False otherwise.
        """
        # recompute balance from ledger amounts
        bal = Decimal("0")
        recomputed_shares: Dict[str, Decimal] = {}
        for e in self._ledger:
            bal += Decimal(e.amount)
            sd = e.details.get("shares_delta")
            sc = e.details.get("share_class")
            if sd is not None and sc is not None:
                recomputed_shares[sc] = recomputed_shares.get(
                    sc, Decimal("0")
                ) + Decimal(sd)

        # quantize to same scale we use for balance
        bal = bal.quantize(Decimal("0.00000001"))
        if bal != self._balance:
            return False
        # compare shares per class
        for sc, v in recomputed_shares.items():
            if self._shares.get(sc, Decimal("0")) != v:
                return False
        # ensure no extra shares in state absent in ledger
        for sc, v in self._shares.items():
            if recomputed_shares.get(sc, Decimal("0")) != v:
                return False
        return True
