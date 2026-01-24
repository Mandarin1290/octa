import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


def _canonical_serialize(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _compute_hash(obj: Any) -> str:
    return hashlib.sha256(_canonical_serialize(obj).encode("utf-8")).hexdigest()


@dataclass
class Order:
    id: str
    strategy: str
    symbol: str
    qty: float
    side: str  # 'buy' or 'sell'
    ts: float


@dataclass
class Fill:
    order_id: str
    qty: float
    price: float
    ts: float


class OrderManagementSystem:
    """Simple OMS with throttling and exposure caps.

    - `max_orders_per_sec` throttles incoming orders per strategy.
    - `exposure_cap` caps notional exposure per strategy; orders that would exceed cap are rejected.
    - Dedup protection prevents duplicate exposure from replayed fills (idempotent by order id).
    """

    def __init__(self, max_orders_per_sec: int = 10, exposure_cap: float = 10000.0):
        self.max_orders_per_sec = max_orders_per_sec
        self.exposure_cap = exposure_cap
        self._recv_timestamps: Dict[str, List[float]] = {}
        self._fills: Dict[str, Fill] = {}
        self._positions: Dict[str, Dict[str, float]] = {}  # strategy -> symbol -> qty
        self._pending_orders: Dict[
            str, Dict[str, float]
        ] = {}  # strategy -> symbol -> qty pending
        self.audit_log: List[Dict[str, Any]] = []

    def _log(self, actor: str, action: str, details: Optional[Dict[str, Any]] = None):
        self.audit_log.append(
            {
                "ts": _now_iso(),
                "actor": actor,
                "action": action,
                "details": details or {},
            }
        )

    def _notional(
        self, strategy: str, extra: Optional[Dict[str, float]] = None
    ) -> float:
        pos = self._positions.get(strategy, {})
        total = 0.0
        for _s, q in pos.items():
            # assume unit price 1 for simplicity in tests; realistic integration would use market prices
            total += abs(q)
        # include pending orders
        pend = self._pending_orders.get(strategy, {})
        for _s, q in pend.items():
            total += abs(q)
        if extra:
            for _s, q in extra.items():
                total += abs(q)
        return total

    def _throttle_check(self, strategy: str, now_ts: float) -> bool:
        lst = self._recv_timestamps.setdefault(strategy, [])
        # keep last 1 second window
        cutoff = now_ts - 1.0
        while lst and lst[0] < cutoff:
            lst.pop(0)
        return len(lst) < self.max_orders_per_sec

    def receive_order(
        self, strategy: str, symbol: str, qty: float, side: str
    ) -> Dict[str, Any]:
        now_ts = time.time()
        allowed = self._throttle_check(strategy, now_ts)
        self._recv_timestamps.setdefault(strategy, []).append(now_ts)
        order_id = str(uuid.uuid4())
        Order(
            id=order_id, strategy=strategy, symbol=symbol, qty=qty, side=side, ts=now_ts
        )

        if not allowed:
            self._log(
                "oms", "throttle_reject", {"strategy": strategy, "order_id": order_id}
            )
            return {"accepted": False, "reason": "throttled", "order_id": order_id}

        # exposure cap check (assume price=1 for notional)
        extra = {symbol: qty}
        new_notional = self._notional(strategy, extra=extra)
        if new_notional > self.exposure_cap:
            self._log(
                "oms",
                "exposure_reject",
                {
                    "strategy": strategy,
                    "order_id": order_id,
                    "new_notional": new_notional,
                },
            )
            return {"accepted": False, "reason": "exposure_cap", "order_id": order_id}

        # mark pending notional for accepted orders until fills arrive
        pend = self._pending_orders.setdefault(strategy, {})
        pend[symbol] = pend.get(symbol, 0.0) + qty

        self._log("oms", "order_accepted", {"strategy": strategy, "order_id": order_id})
        return {"accepted": True, "order_id": order_id}

    def record_fill(
        self, order_id: str, strategy: str, symbol: str, qty: float, price: float
    ) -> Dict[str, Any]:
        # idempotent: ignore duplicate fills with same order_id
        if order_id in self._fills:
            self._log("oms", "duplicate_fill_ignored", {"order_id": order_id})
            return {"ok": True, "duplicate": True}

        now_ts = time.time()
        fill = Fill(order_id=order_id, qty=qty, price=price, ts=now_ts)
        self._fills[order_id] = fill
        pos = self._positions.setdefault(strategy, {})
        pos[symbol] = pos.get(symbol, 0.0) + qty
        # remove pending notional for this order if present
        pend = self._pending_orders.get(strategy, {})
        if symbol in pend:
            pend[symbol] = max(0.0, pend.get(symbol, 0.0) - qty)
            if pend[symbol] == 0.0:
                del pend[symbol]
        self._log(
            "oms",
            "fill_recorded",
            {"order_id": order_id, "strategy": strategy, "symbol": symbol, "qty": qty},
        )
        return {"ok": True, "duplicate": False}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        # simple cancel (no-op for this sim)
        # best-effort: if order was pending, remove pending notional
        # (in this simple model we don't track order->symbol mapping persistently)
        self._log("oms", "cancel", {"order_id": order_id})
        return {"ok": True}


class ExchangeSimulator:
    """Simulate exchange behavior for rejection storms and cancels."""

    def __init__(self, reject_rate: float = 0.0):
        self.reject_rate = reject_rate

    def send_order(self, order: Order) -> Dict[str, Any]:
        import random

        if random.random() < self.reject_rate:
            return {"accepted": False, "reason": "exchange_reject"}
        # accepted -> return fill immediately for simplicity
        return {"accepted": True, "fill_qty": order.qty, "fill_price": 1.0}


__all__ = ["OrderManagementSystem", "ExchangeSimulator", "Order", "Fill"]
