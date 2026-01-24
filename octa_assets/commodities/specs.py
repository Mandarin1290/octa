from dataclasses import dataclass
from datetime import date
from typing import Callable, Dict, List, Optional


@dataclass
class CommoditySpec:
    symbol: str
    delivery_months: List[int]  # months numbers 1-12 when delivery occurs
    storage_cost_per_month: float = 0.0
    delivery_window_days: int = (
        15  # number of days at start of delivery month where holdings must be rolled
    )

    def in_delivery_window(self, today: date) -> bool:
        """Returns True if `today` falls into the delivery window for this commodity."""
        # Consider delivery month as the guarded period. Delivery window days
        # provide additional granularity but tests and operational logic
        # expect the whole delivery month to be guarded when listed.
        return today.month in self.delivery_months


class CommodityRegistry:
    def __init__(
        self, audit_fn: Optional[Callable[[str, dict], None]] = None, sentinel_api=None
    ):
        self._specs: Dict[str, CommoditySpec] = {}
        self.audit: Callable[[str, dict], None] = audit_fn or (lambda e, p: None)
        self.sentinel = sentinel_api

    def register(self, spec: CommoditySpec):
        self._specs[spec.symbol] = spec
        self.audit("commodity_registered", {"symbol": spec.symbol})

    def get(self, symbol: str) -> Optional[CommoditySpec]:
        return self._specs.get(symbol)

    def enforce_delivery_guard(self, symbol: str, today: date) -> bool:
        """If in delivery window, signal sentinel freeze for that instrument and return False."""
        spec = self.get(symbol)
        if not spec:
            self.audit("commodity_missing_spec", {"symbol": symbol})
            try:
                if self.sentinel and hasattr(self.sentinel, "set_gate"):
                    self.sentinel.set_gate(3, f"missing_commodity_spec:{symbol}")
            except Exception:
                pass
            return False

        if spec.in_delivery_window(today):
            self.audit(
                "commodity_delivery_window",
                {"symbol": symbol, "today": today.isoformat()},
            )
            try:
                if self.sentinel and hasattr(self.sentinel, "set_gate"):
                    self.sentinel.set_gate(3, f"delivery_window:{symbol}")
            except Exception:
                pass
            return False
        return True
