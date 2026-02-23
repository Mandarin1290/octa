
from __future__ import annotations
# Minimaler OrderState für Kompatibilität mit ExecutionStateMachine und Tests
from enum import Enum
from dataclasses import dataclass, field

class OrderStatus(str, Enum):
    NEW = "NEW"
    SUBMITTED = "SUBMITTED"
    ACK = "ACK"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    ERROR = "ERROR"

@dataclass
class OrderState:
    order_id: str
    status: OrderStatus = OrderStatus.NEW
    filled_qty: float = 0.0
    avg_price: float = 0.0


import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from .fills import Fill
from .orders import ExecutionReport, OrderRequest


class ExecutionStateStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def load(self) -> "ExecutionState":
        if not self.path.exists():
            return ExecutionState()
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return ExecutionState()
        orders = {}
        for order_id, payload in (raw.get("orders") or {}).items():
            status = payload.get("status") if isinstance(payload, dict) else None
            try:
                status_enum = OrderStatus(status) if status else OrderStatus.NEW
            except Exception:
                status_enum = OrderStatus.NEW
            orders[str(order_id)] = OrderState(
                order_id=str(order_id),
                status=status_enum,
                filled_qty=float(payload.get("filled_qty", 0.0) or 0.0) if isinstance(payload, dict) else 0.0,
                avg_price=float(payload.get("avg_price", 0.0) or 0.0) if isinstance(payload, dict) else 0.0,
            )
        state = ExecutionState(
            open_orders=orders,
            fills=list(raw.get("fills") or []),
            reports=list(raw.get("reports") or []),
        )
        positions = raw.get("positions")
        if isinstance(positions, dict):
            state.positions = {str(k): float(v) for k, v in positions.items()}
        return state

    def save(self, state: "ExecutionState") -> None:
        data = {
            "orders": {
                oid: {
                    "status": order.status.value,
                    "filled_qty": float(order.filled_qty),
                    "avg_price": float(order.avg_price),
                }
                for oid, order in (state.orders or {}).items()
            },
            "fills": list(getattr(state, "fills", []) or []),
            "reports": list(getattr(state, "reports", []) or []),
            "positions": dict(getattr(state, "positions", {}) or {}),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            tmp.write_text(json.dumps(data, sort_keys=True), encoding="utf-8")
            tmp.replace(self.path)
        except Exception:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass


class IdempotencyStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}
        except Exception:
            return {}

    def save(self, data: Dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            tmp.write_text(json.dumps(data, sort_keys=True), encoding="utf-8")
            tmp.replace(self.path)
        except Exception:
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass

    def get(self, key: str) -> Optional[str]:
        data = self.load()
        val = data.get(str(key))
        return str(val) if val is not None else None

    def set(self, key: str, value: str) -> None:
        data = self.load()
        data[str(key)] = str(value)
        self.save(data)



@dataclass
class ExecutionState:
    open_orders: dict[str, OrderState] = field(default_factory=dict)
    fills: list = field(default_factory=list)
    reports: list = field(default_factory=list)
    # Kompatibilitäts-API für Tests und StateMachine
    @property
    def orders(self) -> dict[str, OrderState]:
        return self.open_orders
    @orders.setter
    def orders(self, value: dict[str, OrderState]):
        self.open_orders = value
