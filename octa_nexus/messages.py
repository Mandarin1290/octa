from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Type


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class BaseMessage:
    id: str
    type: str
    ts: str

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "BaseMessage":
        return cls(**d)


@dataclass
class SignalEvent(BaseMessage):
    model: str
    symbol: str
    score: float


@dataclass
class PortfolioIntent(BaseMessage):
    account: str
    symbol: str
    side: str
    qty: float


@dataclass
class RiskDecision(BaseMessage):
    decision: str
    reason: str


@dataclass
class OrderIntent(BaseMessage):
    order_id: str
    symbol: str
    side: str
    qty: float
    price: float


@dataclass
class OrderStatusMsg(BaseMessage):
    order_id: str
    status: str
    filled_qty: float = 0.0


@dataclass
class Healthbeat(BaseMessage):
    component: str
    epoch: str


@dataclass
class Incident(BaseMessage):
    component: str
    severity: str
    msg: str


_TYPE_MAP: Dict[str, Type[BaseMessage]] = {
    "SignalEvent": SignalEvent,
    "PortfolioIntent": PortfolioIntent,
    "RiskDecision": RiskDecision,
    "OrderIntent": OrderIntent,
    "OrderStatus": OrderStatusMsg,
    "Healthbeat": Healthbeat,
    "Incident": Incident,
}


def dumps(msg: BaseMessage) -> str:
    return msg.to_json()


def loads(s: str) -> BaseMessage:
    d = json.loads(s)
    t = d.get("type")
    cls = _TYPE_MAP.get(t)
    if not cls:
        return BaseMessage(**d)
    return cls.from_dict(d)
