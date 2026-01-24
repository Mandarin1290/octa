from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict


class RedemptionGateManager:
    """Enforce redemption gates as a fraction of AUM.

    Example: gate_pct=Decimal('0.05') allows redemptions up to 5% of AUM.
    """

    def __init__(self, gate_pct: Decimal):
        self.gate_pct = Decimal(gate_pct)
        if not (Decimal("0") <= self.gate_pct <= Decimal("1")):
            raise ValueError("gate_pct must be between 0 and 1")

    def allowed_amount(self, aum_total: Decimal) -> Decimal:
        return (Decimal(aum_total) * self.gate_pct).quantize(Decimal("0.00000001"))

    def allow_redemption(self, requested_amount: Decimal, aum_total: Decimal) -> bool:
        return Decimal(requested_amount) <= self.allowed_amount(aum_total)


class LockupManager:
    """Track lockups per investor/share_class. Locks prevent redemption of locked shares until expiry.

    Stores locks as: locks[investor_id][share_class] = list of (shares, unlock_time)
    """

    def __init__(self):
        self.locks: Dict[str, Dict[str, list]] = {}

    def add_lock(
        self, investor_id: str, share_class: str, shares, lock_period_days: int
    ):
        s = Decimal(shares)
        if s <= 0:
            raise ValueError("locked shares must be positive")
        unlock = datetime.now(timezone.utc) + timedelta(days=lock_period_days)
        self.locks.setdefault(investor_id, {}).setdefault(share_class, []).append(
            (s, unlock)
        )

    def _locked_amount(self, investor_id: str, share_class: str) -> Decimal:
        now = datetime.now(timezone.utc)
        total = Decimal("0")
        for s, unlock in list(self.locks.get(investor_id, {}).get(share_class, [])):
            if unlock > now:
                total += s
        return total

    def is_redeemable(self, investor, share_class: str, shares: Decimal) -> bool:
        """Return True if the requested `shares` can be redeemed given lockups.

        `investor` expected to have `investor_id` and `shares` mapping.
        """
        owned = Decimal(investor.shares.get(share_class, Decimal("0")))
        locked = self._locked_amount(investor.investor_id, share_class)
        available = owned - locked
        return Decimal(shares) <= available
