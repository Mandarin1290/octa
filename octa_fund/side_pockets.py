from decimal import Decimal
from typing import Dict


class SidePocketManager:
    """Manage side-pockets that isolate illiquid shares per investor/share class.

    Side pockets are tracked separately; moving shares into a pocket reduces
    the available main-balance for normal redemptions but does not alter the
    investor's global share ledger directly (logical isolation).
    """

    def __init__(self):
        # structure: pockets[investor_id][share_class][pocket_name] = Decimal
        self.pockets: Dict[str, Dict[str, Dict[str, Decimal]]] = {}

    def move_to_pocket(
        self, investor, share_class: str, pocket_name: str, shares: Decimal
    ):
        s = Decimal(shares)
        if s <= 0:
            raise ValueError("shares must be positive")
        owned = Decimal(investor.shares.get(share_class, Decimal("0")))
        currently_pocketed = sum(
            self.pockets.get(investor.investor_id, {}).get(share_class, {}).values(),
            Decimal("0"),
        )
        if s + currently_pocketed > owned:
            raise ValueError("insufficient shares to move into pocket")
        self.pockets.setdefault(investor.investor_id, {}).setdefault(
            share_class, {}
        ).setdefault(pocket_name, Decimal("0"))
        self.pockets[investor.investor_id][share_class][pocket_name] += s

    def pocketed_shares(self, investor, share_class: str) -> Decimal:
        return sum(
            self.pockets.get(investor.investor_id, {}).get(share_class, {}).values(),
            Decimal("0"),
        )

    def available_main_shares(self, investor, share_class: str) -> Decimal:
        owned = Decimal(investor.shares.get(share_class, Decimal("0")))
        return owned - self.pocketed_shares(investor, share_class)

    def redeem_from_main(
        self, investor, share_class: str, shares: Decimal, price_per_share: Decimal
    ):
        s = Decimal(shares)
        if s <= 0:
            raise ValueError("shares must be positive")
        available = self.available_main_shares(investor, share_class)
        if s > available:
            raise ValueError("requested redemption exceeds available main shares")
        # perform normal sell which adjusts investor ledger
        total = investor.sell_shares(share_class, s, Decimal(price_per_share))
        return total

    def redeem_from_pocket(
        self,
        investor,
        share_class: str,
        pocket_name: str,
        shares: Decimal,
        price_per_share: Decimal,
        approved: bool = False,
    ):
        if not approved:
            raise ValueError("redeeming from a side pocket requires approval")
        s = Decimal(shares)
        pocket = self.pockets.get(investor.investor_id, {}).get(share_class, {})
        if pocket.get(pocket_name, Decimal("0")) < s:
            raise ValueError("insufficient pocketed shares")
        # reduce pocketed amount and perform sale
        self.pockets[investor.investor_id][share_class][pocket_name] -= s
        return investor.sell_shares(share_class, s, Decimal(price_per_share))
