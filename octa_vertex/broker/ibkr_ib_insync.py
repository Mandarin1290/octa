from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from octa_vertex.broker.base import BrokerAdapter


@dataclass
class IBKRIBInsyncConfig:
    host: str = "127.0.0.1"
    port: int = 7497  # IB Gateway/TWS paper default
    client_id: int = 7
    account: Optional[str] = None

    @classmethod
    def from_env(cls) -> "IBKRIBInsyncConfig":
        host = os.getenv("IBKR_HOST", "127.0.0.1")
        port = int(os.getenv("IBKR_PORT", "7497"))
        client_id = int(os.getenv("IBKR_CLIENT_ID", "7"))
        account = os.getenv("IBKR_ACCOUNT")
        return cls(host=host, port=port, client_id=client_id, account=account)


class IBKRIBInsyncAdapter(BrokerAdapter):
    """IBKR adapter via ib_insync.

    Notes:
    - This is only used when explicitly enabled via configuration/env.
    - If ib_insync is not installed or connection fails, callers should fail-closed.
    """

    def __init__(self, cfg: IBKRIBInsyncConfig):
        self.cfg = cfg
        try:
            from ib_insync import IB  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("ib_insync_not_available") from e

        self._IB = IB
        self.ib = IB()
        ok = self.ib.connect(self.cfg.host, int(self.cfg.port), clientId=int(self.cfg.client_id))
        if not ok:
            raise RuntimeError("ibkr_connect_failed")

    def submit_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        # Minimal: Market order for Stock-like instruments.
        # Contract qualification and asset-class routing should be extended for FX/futures/crypto.
        from ib_insync import MarketOrder, Stock  # type: ignore

        instrument = str(order.get("instrument") or "")
        if not instrument:
            raise ValueError("missing instrument")
        qty = float(order.get("qty") or 0.0)
        if qty <= 0:
            raise ValueError("invalid qty")
        side = str(order.get("side") or "").upper()
        action = "BUY" if side == "BUY" else "SELL"

        contract = Stock(instrument, "SMART", "USD")
        self.ib.qualifyContracts(contract)
        o = MarketOrder(action, qty)
        trade = self.ib.placeOrder(contract, o)

        # best-effort immediate status
        status = getattr(getattr(trade, "orderStatus", None), "status", None)
        return {"order_id": str(order.get("order_id")), "status": str(status or "SUBMITTED")}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        # Without holding Trade references, we can only ack the request.
        return {"order_id": str(order_id), "status": "CANCEL_REQUESTED"}

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        return {"order_id": str(order_id), "status": "UNKNOWN"}

    def account_snapshot(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"summary": [], "positions": []}
        try:
            summary = self.ib.accountSummary()
            out["summary"] = [s.dict() if hasattr(s, "dict") else str(s) for s in summary]
        except Exception:
            out["summary"] = []

        # Best-effort positions: do not raise; callers may treat empties as unavailable.
        try:
            pos = self.ib.positions()
            rows = []
            for p in pos:
                try:
                    contract = getattr(p, "contract", None)
                    sym = getattr(contract, "symbol", None) if contract is not None else None
                    rows.append(
                        {
                            "symbol": str(sym or ""),
                            "qty": float(getattr(p, "position", 0.0) or 0.0),
                            "avg_cost": float(getattr(p, "avgCost", 0.0) or 0.0),
                        }
                    )
                except Exception:
                    continue
            out["positions"] = rows
        except Exception:
            out["positions"] = []

        return out
