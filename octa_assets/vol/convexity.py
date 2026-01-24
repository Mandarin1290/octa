from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple


@dataclass
class VolPosition:
    instrument: str
    type: str  # 'future' or 'etf' or 'option'
    qty: float  # positive implies long vol, negative implies short vol
    notional: float  # monetary notional for aggregation
    convexity_proxy: float  # per-unit convexity proxy (positive for long vol)


class ConvexityTracker:
    def __init__(
        self, audit_fn: Optional[Callable[[str, dict], None]] = None, sentinel_api=None
    ):
        self.positions: List[VolPosition] = []
        self.audit = audit_fn or (lambda e, p: None)
        self.sentinel = sentinel_api

    def record(self, pos: VolPosition):
        self.positions.append(pos)
        self.audit(
            "vol_position_recorded",
            {"instrument": pos.instrument, "qty": pos.qty, "notional": pos.notional},
        )

    def aggregate_convexity(self) -> Dict[str, float]:
        """Return total convexity proxy and short-vol notional.

        convexity proxy = sum(pos.qty * pos.convexity_proxy * pos.notional) ; short_vol_notional = sum(abs(notional) for short positions)
        """
        total_convex = 0.0
        short_notional = 0.0
        for p in self.positions:
            total_convex += float(p.qty) * float(p.convexity_proxy) * float(p.notional)
            if p.qty < 0:
                short_notional += abs(p.notional)
        return {"convexity": total_convex, "short_notional": short_notional}

    def enforce_short_vol_cap(
        self, cap_notional: float
    ) -> Tuple[bool, Dict[str, float]]:
        agg = self.aggregate_convexity()
        if agg["short_notional"] > cap_notional:
            try:
                if self.sentinel and hasattr(self.sentinel, "set_gate"):
                    self.sentinel.set_gate(
                        2, f"short_vol_cap_breach:{agg['short_notional']}"
                    )
            except Exception:
                pass
            self.audit(
                "short_vol_cap_breach",
                {"short_notional": agg["short_notional"], "cap": cap_notional},
            )
            return False, {"short_notional": agg["short_notional"]}
        return True, {}
