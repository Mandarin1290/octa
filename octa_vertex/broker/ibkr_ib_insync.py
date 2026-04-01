from __future__ import annotations

import os
import time
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
        # How long to wait for a fill after placeOrder (seconds). Configurable via env.
        self.fill_timeout_s: int = int(os.getenv("OCTA_FILL_TIMEOUT_S", "30"))

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

        # Wait for fill confirmation (up to fill_timeout_s).
        # Market orders on paper/live accounts typically fill within seconds.
        # Outside market hours the status stays Submitted; we return fill_price=None.
        fill_price: Optional[float] = None
        fill_qty: Optional[float] = None
        deadline = time.time() + self.fill_timeout_s
        while time.time() < deadline:
            self.ib.sleep(0.5)
            current_status = str(getattr(getattr(trade, "orderStatus", None), "status", None) or "")
            if current_status == "Filled":
                fills = getattr(trade, "fills", None) or []
                if fills:
                    exec_obj = getattr(fills[-1], "execution", None)
                    if exec_obj is not None:
                        raw_price = getattr(exec_obj, "avgPrice", None)
                        raw_shares = getattr(exec_obj, "shares", None)
                        fill_price = float(raw_price) if raw_price is not None else None
                        fill_qty = float(raw_shares) if raw_shares is not None else None
                break
            if current_status in ("Cancelled", "Inactive", "ApiCancelled", "ApiPendingCancel"):
                break

        final_status = str(getattr(getattr(trade, "orderStatus", None), "status", None) or "SUBMITTED")
        return {
            "order_id": str(order.get("order_id")),
            "status": final_status,
            "fill_price": fill_price,
            "fill_qty": fill_qty,
        }

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        # Without holding Trade references, we can only ack the request.
        return {"order_id": str(order_id), "status": "CANCEL_REQUESTED"}

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        return {"order_id": str(order_id), "status": "UNKNOWN"}

    def get_positions(self) -> Dict[str, Any]:
        """Return current broker positions as {symbol: {qty, avg_cost}}.

        Uses account_snapshot() positions list as source of truth.
        Returns empty dict on any error (caller should treat as unavailable).
        """
        try:
            snap = self.account_snapshot()
            result: Dict[str, Any] = {}
            for p in snap.get("positions") or []:
                sym = str(p.get("symbol") or "")
                if sym:
                    result[sym] = {
                        "qty": float(p.get("qty", 0.0)),
                        "avg_cost": float(p.get("avg_cost", 0.0)),
                    }
            return result
        except Exception:
            return {}

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
