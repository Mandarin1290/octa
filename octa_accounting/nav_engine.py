from __future__ import annotations

import datetime
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class NAVEngine:
    cash: float = 0.0
    positions: Dict[str, float] = field(default_factory=dict)
    cost_basis: Dict[str, float] = field(default_factory=dict)
    realized_pnl: float = 0.0
    fees_accrued: float = 0.0
    market_prices: Dict[str, float] = field(default_factory=dict)
    audit_log: List[Dict[str, Any]] = field(default_factory=list)
    history: List[Dict[str, Any]] = field(default_factory=list)

    def _now(self) -> str:
        return datetime.datetime.now(datetime.timezone.utc).isoformat()

    def _record(self, actor: str, action: str, details: Dict[str, Any]) -> None:
        entry = {
            "ts": self._now(),
            "actor": actor,
            "action": action,
            "details": details,
        }
        self.audit_log.append(entry)
        # history stores events for deterministic replay
        self.history.append({"ts": entry["ts"], "action": action, "details": details})

    def deposit(self, amount: float, actor: str = "system") -> None:
        self.cash = round(self.cash + float(amount), 8)
        self._record(actor, "deposit", {"amount": amount, "new_cash": self.cash})

    def withdraw(self, amount: float, actor: str = "system") -> None:
        amount = float(amount)
        if amount > self.cash:
            raise ValueError("insufficient_cash")
        self.cash = round(self.cash - amount, 8)
        self._record(actor, "withdraw", {"amount": amount, "new_cash": self.cash})

    def record_trade(
        self,
        symbol: str,
        qty: float,
        price: float,
        actor: str = "system",
        fee: Optional[float] = None,
    ) -> None:
        """Record a trade; positive qty = buy, negative = sell.

        Realized PnL is computed on sells against cost_basis.
        """
        qty = float(qty)
        price = float(price)

        cash_change = -qty * price
        self.cash = round(self.cash + cash_change, 8)

        prev_qty = self.positions.get(symbol, 0.0)
        prev_cost = self.cost_basis.get(symbol, 0.0)

        new_qty = prev_qty + qty

        # if reducing position (sell)
        if prev_qty != 0 and (prev_qty > 0 and qty < 0 or prev_qty < 0 and qty > 0):
            # compute realized on closed qty
            closed = min(abs(qty), abs(prev_qty))
            # realized = closed * (sell_price - cost_basis_sign)
            realized = 0.0
            if prev_qty > 0:
                realized = closed * (price - prev_cost)
            else:
                realized = closed * (prev_cost - price)
            self.realized_pnl = round(self.realized_pnl + realized, 8)

        # update cost basis for increased position
        if new_qty != 0 and qty > 0:
            # weighted average cost for buys
            total_cost = prev_cost * prev_qty + price * qty
            self.cost_basis[symbol] = (
                round(total_cost / new_qty, 8) if new_qty != 0 else 0.0
            )
        elif new_qty == 0:
            # fully closed
            self.cost_basis[symbol] = 0.0

        self.positions[symbol] = round(new_qty, 8)

        if fee:
            self.accrue_fee(fee, actor=actor)

        self._record(
            actor,
            "trade",
            {
                "symbol": symbol,
                "qty": qty,
                "price": price,
                "fee": fee,
                "cash": self.cash,
                "positions": dict(self.positions),
            },
        )

    def update_market_price(
        self, symbol: str, price: float, actor: str = "system"
    ) -> None:
        self.market_prices[symbol] = float(price)
        self._record(actor, "price_update", {"symbol": symbol, "price": price})

    def accrue_fee(self, amount: float, actor: str = "system") -> None:
        amount = float(amount)
        self.fees_accrued = round(self.fees_accrued + amount, 8)
        self.cash = round(self.cash - amount, 8)
        self._record(
            actor,
            "fee_accrued",
            {"amount": amount, "fees_total": self.fees_accrued, "cash": self.cash},
        )

    def unrealized_pnl(self) -> float:
        total = 0.0
        for sym, qty in self.positions.items():
            price = self.market_prices.get(sym)
            if price is None:
                continue
            cost = self.cost_basis.get(sym, 0.0)
            total += qty * (price - cost)
        return round(total, 8)

    def compute_nav(self) -> Dict[str, Any]:
        mv = 0.0
        for sym, qty in self.positions.items():
            price = self.market_prices.get(sym)
            if price is not None:
                mv += qty * price

        nav = round(self.cash + mv, 8)
        report = {
            "cash": self.cash,
            "market_value": round(mv, 8),
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl(),
            "fees_accrued": self.fees_accrued,
            "nav": nav,
            "positions": dict(self.positions),
            "cost_basis": dict(self.cost_basis),
            "market_prices": dict(self.market_prices),
        }
        # deterministic report hash
        report_hash = canonical_hash(report)
        report["report_hash"] = report_hash
        self._record(
            "system", "nav_snapshot", {"report_hash": report_hash, "report": report}
        )
        return report

    def replay_history(self, events: List[Dict[str, Any]]) -> None:
        # Replay deterministic events in order to reconstruct state
        for e in events:
            action = e.get("action") or e.get("action_type") or e.get("type")
            details = e.get("details") or {}
            if action == "deposit":
                self.deposit(
                    details.get("amount", 0.0), actor=details.get("actor", "replay")
                )
            elif action == "withdraw":
                self.withdraw(
                    details.get("amount", 0.0), actor=details.get("actor", "replay")
                )
            elif action == "trade":
                self.record_trade(
                    details["symbol"],
                    details["qty"],
                    details["price"],
                    actor=details.get("actor", "replay"),
                    fee=details.get("fee"),
                )
            elif action == "price_update":
                self.update_market_price(
                    details["symbol"],
                    details["price"],
                    actor=details.get("actor", "replay"),
                )
            elif action == "fee_accrued":
                self.accrue_fee(
                    details.get("amount", 0.0), actor=details.get("actor", "replay")
                )
