import uuid
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from typing import Any, Callable, Dict, List

getcontext().prec = 12

AuditFn = Callable[[str, Dict[str, Any]], None]


def _noop_audit(event: str, payload: Dict[str, Any]) -> None:
    return None


class SubscriptionManager:
    """Manage subscription bookings executed at NAV.

    Usage:
    - `request_subscription(investor, share_class, amount)` records a pending
      subscription to be executed at the next NAV.
    - `process_subscriptions(nav_price)` executes pending subscriptions at the
      provided `nav_price` (Decimal) and mints shares accordingly.
    """

    def __init__(self, audit_fn: AuditFn = _noop_audit):
        self.pending: List[Dict[str, Any]] = []
        self.audit_fn = audit_fn

    def request_subscription(self, investor, share_class: str, amount: Decimal) -> str:
        amt = Decimal(amount)
        if amt <= 0:
            raise ValueError("subscription amount must be positive")
        sid = str(uuid.uuid4())
        rec = {
            "id": sid,
            "investor": investor,
            "share_class": share_class,
            "amount": amt,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.pending.append(rec)
        try:
            self.audit_fn(
                "subscription.requested",
                {"id": sid, "investor_id": investor.investor_id, "amount": str(amt)},
            )
        except Exception:
            pass
        return sid

    def process_subscriptions(self, nav_price: Decimal) -> List[str]:
        executed: List[str] = []
        nav = Decimal(nav_price)
        if nav <= 0:
            raise ValueError("nav_price must be positive")
        for rec in list(self.pending):
            investor = rec["investor"]
            amt = rec["amount"]
            share_class = rec["share_class"]
            # Book cash then purchase shares at NAV
            investor.deposit(amt, memo=f"subscription:{rec['id']}")
            shares = (amt / nav).quantize(Decimal("0.00000001"))
            investor.buy_shares(share_class, shares, nav)
            executed.append(rec["id"])
            try:
                self.audit_fn(
                    "subscription.processed",
                    {
                        "id": rec["id"],
                        "investor_id": investor.investor_id,
                        "shares": str(shares),
                        "nav": str(nav),
                    },
                )
            except Exception:
                pass
            self.pending.remove(rec)
        return executed


class RedemptionManager:
    """Manage redemption requests and a queue respecting liquidity.

    Redemptions are only processed when there is sufficient liquid capital; if
    not, they remain in the queue. No forced liquidation is performed.
    """

    def __init__(self, audit_fn: AuditFn = _noop_audit):
        self.queue: List[Dict[str, Any]] = []
        self.audit_fn = audit_fn

    def request_redemption(self, investor, share_class: str, shares: Decimal) -> str:
        s = Decimal(shares)
        if s <= 0:
            raise ValueError("shares must be positive")
        rid = str(uuid.uuid4())
        rec = {
            "id": rid,
            "investor": investor,
            "share_class": share_class,
            "shares": s,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.queue.append(rec)
        try:
            self.audit_fn(
                "redemption.requested",
                {"id": rid, "investor_id": investor.investor_id, "shares": str(s)},
            )
        except Exception:
            pass
        return rid

    def process_redemptions(
        self, nav_price: Decimal, available_liquid: Decimal
    ) -> List[str]:
        """Attempt to process queued redemptions in FIFO order.

        `nav_price` is Decimal price per share. `available_liquid` is Decimal
        representing immediately available cash. Returns list of processed ids.
        """
        nav = Decimal(nav_price)
        liquid = Decimal(available_liquid)
        processed: List[str] = []
        for rec in list(self.queue):
            investor = rec["investor"]
            shares = Decimal(rec["shares"])
            required = (shares * nav).quantize(Decimal("0.00000001"))
            if required <= liquid:
                # perform sell then withdraw to represent payout
                investor.sell_shares(rec["share_class"], shares, nav)
                investor.withdraw(required, memo=f"redemption:{rec['id']}")
                liquid -= required
                processed.append(rec["id"])
                try:
                    self.audit_fn(
                        "redemption.processed",
                        {
                            "id": rec["id"],
                            "investor_id": investor.investor_id,
                            "shares": str(shares),
                            "nav": str(nav),
                        },
                    )
                except Exception:
                    pass
                self.queue.remove(rec)
            else:
                try:
                    self.audit_fn(
                        "redemption.queued",
                        {
                            "id": rec["id"],
                            "investor_id": investor.investor_id,
                            "required": str(required),
                            "available": str(liquid),
                        },
                    )
                except Exception:
                    pass
                # do not attempt further redemptions if liquidity insufficient
                break
        return processed
