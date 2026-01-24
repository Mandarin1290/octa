from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from octa_vertex.broker.base import BrokerAdapter


@dataclass
class IBKRConfig:
    rate_limit_per_minute: int = 60
    allowed_order_types: List[str] | None = None
    supported_instruments: List[str] | None = None

    def __post_init__(self):
        if self.allowed_order_types is None:
            self.allowed_order_types = ["MKT", "LMT", "STP"]
        if self.supported_instruments is None:
            self.supported_instruments = []


class IBKRContractAdapter(BrokerAdapter):
    """Strict interface layer for IBKR semantics (sandbox-only).

    Behavior:
      - validates order fields and instrument qualification
      - enforces simple rate limit
      - returns structured rejection reasons
    """

    def __init__(
        self, config: IBKRConfig | None = None, audit_fn=None, sentinel_api=None
    ):
        self.cfg = config or IBKRConfig()
        self.audit = audit_fn or (lambda evt, p: None)
        self.sentinel = sentinel_api
        # simple timestamps for rate limiting
        self._timestamps: List[float] = []
        self._orders: Dict[str, Dict[str, Any]] = {}

    def _enforce_rate_limit(self) -> Optional[Dict[str, Any]]:
        now = time.time()
        window = 60.0
        # drop old
        self._timestamps = [t for t in self._timestamps if now - t <= window]
        if len(self._timestamps) >= self.cfg.rate_limit_per_minute:
            return {"status": "REJECTED", "reason": "RATE_LIMIT_EXCEEDED"}
        self._timestamps.append(now)
        return None

    def _validate_order(self, order: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # required fields
        required = ["order_id", "instrument", "qty", "side", "order_type"]
        for r in required:
            if r not in order:
                return {"status": "REJECTED", "reason": f"MISSING_{r.upper()}"}

        if order["order_type"] not in (self.cfg.allowed_order_types or []):
            return {"status": "REJECTED", "reason": "UNSUPPORTED_ORDER_TYPE"}

        if order["instrument"] not in (self.cfg.supported_instruments or []):
            return {"status": "REJECTED", "reason": "INSTRUMENT_NOT_QUALIFIED"}

        # simple qty validation
        if order["qty"] <= 0:
            return {"status": "REJECTED", "reason": "INVALID_QTY"}

        return None

    def submit_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        # audit incoming
        self.audit("broker.submit_attempt", {"order_id": order.get("order_id")})

        rl = self._enforce_rate_limit()
        if rl is not None:
            # rate-limit: notify sentinel
            if self.sentinel:
                try:
                    self.sentinel.set_gate(2, "broker_rate_limit")
                except Exception:
                    pass
            self.audit("broker.submit_reject", rl)
            return rl

        v = self._validate_order(order)
        if v is not None:
            self.audit("broker.submit_reject", v)
            return v

        # accept into local simulated book
        oid = order["order_id"]
        self._orders[oid] = {"order": order, "status": "PENDING"}
        self.audit("broker.order_ack", {"order_id": oid, "status": "PENDING"})
        return {"order_id": oid, "status": "PENDING"}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        self.audit("broker.cancel_attempt", {"order_id": order_id})
        o = self._orders.get(order_id)
        if not o:
            res = {"order_id": order_id, "status": "UNKNOWN_ORDER"}
            self.audit("broker.cancel_reject", res)
            return res
        o["status"] = "CANCELLED"
        self.audit("broker.cancel_ack", {"order_id": order_id, "status": "CANCELLED"})
        return {"order_id": order_id, "status": "CANCELLED"}

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        o = self._orders.get(order_id)
        if not o:
            return {"order_id": order_id, "status": "UNKNOWN_ORDER"}
        return {"order_id": order_id, "status": o["status"]}

    def account_snapshot(self) -> Dict[str, Any]:
        # sandbox snapshot: empty positions, conservative buying power
        snap = {
            "positions": [],
            "margin": {"initial": 0.0, "maintenance": 0.0},
            "buying_power": 0.0,
        }
        self.audit("broker.account_snapshot", snap)
        return snap
