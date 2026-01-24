from collections import defaultdict
from typing import Callable, Dict, Optional, Tuple


class ExposureTracker:
    """Track base and quote exposures per account/strategy and provide netting.

    - Exposures are tracked per currency.
    - FX trades are two-legged: base exposure += base_qty, quote exposure += -base_qty*price
    """

    def __init__(
        self, audit_fn: Optional[Callable[[str, dict], None]] = None, sentinel_api=None
    ):
        self.audit = audit_fn or (lambda e, p: None)
        self.sentinel = sentinel_api
        # exposures: mapping (account, strategy) -> currency -> exposure
        self._exposures: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(
            lambda: defaultdict(float)
        )

    def record_trade(
        self,
        account: str,
        strategy: str,
        base_currency: str,
        quote_currency: str,
        base_qty: float,
        price: float,
    ):
        key = (account, strategy)
        # base exposure in base currency
        self._exposures[key][base_currency] += float(base_qty)
        # quote exposure in quote currency (negative when buying base)
        self._exposures[key][quote_currency] += -float(base_qty) * float(price)
        self.audit(
            "fx_trade_recorded",
            {
                "account": account,
                "strategy": strategy,
                "base": base_currency,
                "quote": quote_currency,
                "base_qty": base_qty,
                "price": price,
            },
        )

    def net_exposure(self) -> Dict[str, float]:
        """Net exposures aggregated across all accounts/strategies by currency."""
        agg: Dict[str, float] = defaultdict(float)
        for _k, curmap in self._exposures.items():
            for cur, val in curmap.items():
                agg[cur] += val
        return dict(agg)

    def exposure_by_entity(self, account: str, strategy: str) -> Dict[str, float]:
        return dict(self._exposures.get((account, strategy), {}))

    def enforce_caps(self, caps: Dict[str, float]) -> Tuple[bool, Dict[str, float]]:
        """Check net exposures against caps (abs values). If breach, notify sentinel and return (False, breaches)."""
        net = self.net_exposure()
        breaches = {}
        for cur, val in net.items():
            cap = caps.get(cur)
            if cap is not None and abs(val) > cap:
                breaches[cur] = val
        if breaches:
            try:
                if self.sentinel and hasattr(self.sentinel, "set_gate"):
                    self.sentinel.set_gate(2, f"fx_exposure_breach:{breaches}")
            except Exception:
                pass
            self.audit("fx_exposure_breach", {"breaches": breaches})
            return False, breaches
        return True, {}
