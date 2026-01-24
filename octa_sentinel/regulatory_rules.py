from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple


class RegulatoryRuleEngine:
    def __init__(
        self,
        config: Dict | None = None,
        sentinel_api=None,
        audit_fn: Optional[Callable[[str, dict], None]] = None,
    ):
        cfg = dict(config or {})
        self.require_locate = bool(cfg.get("require_locate", True))
        self.cancel_threshold = int(cfg.get("cancel_threshold", 100))
        self.cancel_window = timedelta(
            seconds=int(cfg.get("cancel_window_seconds", 60))
        )
        self.max_order_freq = int(cfg.get("max_order_freq", 200))
        self.order_freq_window = timedelta(
            seconds=int(cfg.get("order_freq_window_seconds", 60))
        )
        self.sentinel = sentinel_api
        self.audit = audit_fn or (lambda e, p: None)

        # in-memory event tracking
        self.cancels: Dict[Tuple[str, str], List[datetime]] = {}
        self.orders: Dict[Tuple[str, str], List[datetime]] = {}
        self.recent_orders: List[Dict[str, Any]] = []

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _prune(self, entries: List[datetime], window: timedelta) -> List[datetime]:
        now = self._now()
        return [t for t in entries if now - t <= window]

    def record_event(self, event_type: str, order: Dict):
        """Record events (ORDER, CANCEL) for pattern detection."""
        acct = order.get("account_id", "_default")
        instr = order.get("instrument") or "_unknown"
        key = (acct, instr)
        now = self._now()

        if event_type == "CANCEL":
            lst = self.cancels.setdefault(key, [])
            lst.append(now)
            self.cancels[key] = self._prune(lst, self.cancel_window)
            if len(self.cancels[key]) > self.cancel_threshold:
                self.audit(
                    "cancel_storm",
                    {
                        "account": acct,
                        "instrument": instr,
                        "count": len(self.cancels[key]),
                    },
                )
                if self.sentinel and hasattr(self.sentinel, "set_gate"):
                    self.sentinel.set_gate(2, f"cancel_storm:{acct}:{instr}")
                return True

        if event_type == "ORDER":
            lst = self.orders.setdefault(key, [])
            lst.append(now)
            self.orders[key] = self._prune(lst, self.order_freq_window)
            if len(self.orders[key]) > self.max_order_freq:
                self.audit(
                    "order_rate_excess",
                    {
                        "account": acct,
                        "instrument": instr,
                        "count": len(self.orders[key]),
                    },
                )
                if self.sentinel and hasattr(self.sentinel, "set_gate"):
                    self.sentinel.set_gate(2, f"order_rate_excess:{acct}:{instr}")
                return True

            # wash-trade self-cross detection: same account, opposite side, same price within window
            price = order.get("price")
            side = order.get("side")
            window = self.order_freq_window
            now = self._now()
            # remove old entries
            self.recent_orders = [
                o for o in self.recent_orders if now - o["ts"] <= window
            ]
            for o in self.recent_orders:
                if (
                    o.get("account_id") == acct
                    and o.get("instrument") == instr
                    and o.get("price") == price
                    and o.get("side") != side
                ):
                    # wash-trade detected
                    self.audit(
                        "wash_trade_detected",
                        {"account": acct, "instrument": instr, "price": price},
                    )
                    if self.sentinel and hasattr(self.sentinel, "set_gate"):
                        # regulatory breach -> freeze
                        self.sentinel.set_gate(3, f"wash_trade:{acct}:{instr}")
                    return True

            self.recent_orders.append(
                {
                    "account_id": acct,
                    "instrument": instr,
                    "price": price,
                    "side": side,
                    "ts": now,
                }
            )

        return False

    def pre_trade_check(
        self,
        order: Dict,
        positions_lookup: Optional[Callable[[str, str], float]] = None,
        locates_lookup: Optional[Callable[[str, str], bool]] = None,
    ) -> Tuple[bool, str]:
        """Run regulatory pre-trade checks. Returns (allowed, reason)."""
        acct = order.get("account_id", "_default")
        instr = order.get("instrument") or "_unknown"
        side = order.get("side", "BUY").upper()
        qty = float(order.get("qty", 0))

        # Short selling locate required
        if side == "SELL" and self.require_locate:
            has_locate = False
            if locates_lookup:
                try:
                    has_locate = bool(locates_lookup(acct, instr))
                except Exception:
                    has_locate = False

            position = 0.0
            if positions_lookup:
                try:
                    position = float(positions_lookup(acct, instr) or 0.0)
                except Exception:
                    position = 0.0

            # naked short if selling more than held and no locate
            if qty > position and not has_locate:
                self.audit(
                    "naked_short_block",
                    {"order": order, "position": position, "has_locate": has_locate},
                )
                if self.sentinel and hasattr(self.sentinel, "set_gate"):
                    # regulatory breach -> immediate freeze
                    self.sentinel.set_gate(3, f"naked_short:{acct}:{instr}")
                return False, "naked_short"

        # rate and pattern checks: record ORDER event and see if it triggers
        triggered = self.record_event("ORDER", order)
        if triggered:
            return False, "pattern_risk"

        return True, "ok"
