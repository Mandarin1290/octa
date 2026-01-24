from dataclasses import dataclass
from datetime import date
from typing import Callable, Dict, Optional


@dataclass
class FuturesContract:
    symbol: str
    root: str
    expiry: date
    multiplier: float
    tick_size: float
    currency: str
    initial_margin: float  # fraction of notional, e.g., 0.05
    maintenance_margin: float  # fraction


class ContractRegistry:
    def __init__(
        self, audit_fn: Optional[Callable[[str, dict], None]] = None, sentinel_api=None
    ):
        self._by_symbol: Dict[str, FuturesContract] = {}
        self.audit = audit_fn or (lambda e, p: None)
        self.sentinel = sentinel_api

    def register(self, contract: FuturesContract):
        self._by_symbol[contract.symbol] = contract
        self.audit(
            "futures_contract_registered",
            {"symbol": contract.symbol, "root": contract.root},
        )

    def get(self, symbol: str) -> Optional[FuturesContract]:
        return self._by_symbol.get(symbol)

    def enforce_exists(self, symbol: str) -> bool:
        c = self.get(symbol)
        if c is None:
            # freeze trading for that instrument
            try:
                if self.sentinel and hasattr(self.sentinel, "set_gate"):
                    self.sentinel.set_gate(3, f"missing_contract:{symbol}")
            except Exception:
                pass
            self.audit("futures_missing_contract", {"symbol": symbol})
            return False
        return True

    def margin_required(self, symbol: str, qty: float, price: float) -> float:
        c = self.get(symbol)
        if c is None:
            raise KeyError("contract not found")
        notional = abs(qty) * c.multiplier * price
        # initial margin requirement
        return notional * float(c.initial_margin)
