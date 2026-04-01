from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from octa_vertex.broker.base import BrokerAdapter

_FOUNDATION_SCOPE_BLOCK_REASON = "real_order_blocked_in_v0_0_0_foundation_scope"


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


def _make_ib_contract(spec: Any) -> Any:
    """Build an ib_insync contract object from a ContractSpec."""
    from ib_insync import Crypto, Forex, Future, Index, Option, Stock  # type: ignore

    ct = spec.contract_type
    if ct == "stock":
        return Stock(spec.symbol, spec.exchange, spec.currency)
    elif ct == "forex":
        return Forex(spec.symbol)
    elif ct == "future":
        return Future(spec.symbol, exchange=spec.exchange, currency=spec.currency)
    elif ct == "option":
        return Option(spec.symbol, exchange=spec.exchange, currency=spec.currency)
    elif ct == "crypto":
        return Crypto(spec.symbol, spec.exchange, spec.currency)
    elif ct == "index":
        return Index(spec.symbol, spec.exchange, spec.currency)
    else:
        return Stock(spec.symbol, spec.exchange, spec.currency)  # fallback: equity


class IBKRIBInsyncAdapter(BrokerAdapter):
    """IBKR adapter via ib_insync.

    Notes:
    - This is only used when explicitly enabled via configuration/env.
    - If ib_insync is not installed or connection fails, callers should fail-closed.
    """

    def __init__(self, cfg: IBKRIBInsyncConfig):
        self.cfg = cfg
        if os.getenv("OCTA_ALLOW_PAPER_ORDERS", "").lower() not in ("1", "true", "yes"):
            raise RuntimeError(
                f"{_FOUNDATION_SCOPE_BLOCK_REASON} — "
                "set OCTA_ALLOW_PAPER_ORDERS=1 to enable paper trading"
            )
        try:
            from ib_insync import IB  # type: ignore
        except ImportError as exc:
            raise RuntimeError("ib_insync not installed") from exc
        self.ib = IB()
        self.ib.connect(cfg.host, cfg.port, clientId=cfg.client_id)

    def submit_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        from ib_insync import MarketOrder  # type: ignore
        from octa_vertex.broker.asset_class_router import resolve_contract_spec

        instrument = str(order.get("instrument") or "")
        if not instrument:
            raise ValueError("missing instrument")
        qty = float(order.get("qty") or 0.0)
        if qty <= 0:
            raise ValueError("invalid qty")
        side = str(order.get("side") or "").upper()
        action = "BUY" if side == "BUY" else "SELL"

        asset_class = str(order.get("asset_class", "equity")).lower().strip()
        spec = resolve_contract_spec(instrument, asset_class)
        contract = _make_ib_contract(spec)
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
