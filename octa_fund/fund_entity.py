from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Optional


def _iso(dt: datetime) -> str:
    return dt.isoformat() + "Z"


@dataclass(frozen=True)
class FundEntity:
    fund_id: str
    name: str
    base_currency: str
    inception_date: str  # ISO date string
    accounting_calendar: str  # e.g., 'monthly', 'quarterly'
    share_classes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    _audit_fn: Optional[Callable[[str, Dict[str, Any]], None]] = field(
        default=None, repr=False
    )

    # non-core, mutable links (not part of frozen identity)
    def attach_aum_state(self, aum_state) -> None:
        # attach AUMState instance for live queries; audited
        object.__setattr__(self, "_aum_state", aum_state)
        if self._audit_fn:
            self._audit_fn(
                "fund.attach_aum", {"fund_id": self.fund_id, "aum_attached": True}
            )

    def attach_nav_engine(self, nav_engine) -> None:
        object.__setattr__(self, "_nav_engine", nav_engine)
        if self._audit_fn:
            self._audit_fn(
                "fund.attach_nav", {"fund_id": self.fund_id, "nav_attached": True}
            )

    def get_current_aum(self) -> float:
        aum = getattr(self, "_aum_state", None)
        if aum is None:
            raise RuntimeError("AUM state not attached")
        return float(aum.get_current_total())

    def compute_nav(self) -> Dict[str, Any]:
        nav = getattr(self, "_nav_engine", None)
        if nav is None:
            raise RuntimeError("NAV engine not attached")
        result = nav.compute_nav(self.share_classes)
        if self._audit_fn:
            self._audit_fn(
                "fund.nav_computed", {"fund_id": self.fund_id, "nav": result}
            )
        return result
