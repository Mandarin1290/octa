from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Optional


def _now_iso():
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class ShareClass:
    class_id: str
    currency: str
    launch_date: str
    shares_outstanding: float = 0.0
    cash_balance: float = 0.0
    assets: Dict[str, float] = field(default_factory=dict)  # asset_id -> market_value
    management_fee_annual: float = 0.0  # e.g., 0.02 for 2% p.a.
    performance_fee: float = 0.0  # e.g., 0.2 for 20% carry over hurdle
    hurdle_rate: float = 0.0  # annual hurdle for performance fee
    high_water_mark: float = 0.0
    audit_fn: Optional[Callable[[str, Dict[str, Any]], None]] = None

    def deposit(self, amount: float) -> None:
        self.cash_balance += float(amount)
        if self.audit_fn:
            self.audit_fn(
                "shareclass.deposit",
                {
                    "class_id": self.class_id,
                    "amount": float(amount),
                    "timestamp": _now_iso(),
                },
            )

    def redeem(self, shares: float) -> float:
        # Redeem by shares: compute NAV and decrease shares and cash
        nav = self.compute_nav()
        redeem_value = shares * nav["nav_per_share"]
        if redeem_value > self.total_value():
            raise RuntimeError("Insufficient liquidity in class")
        # reduce shares and cash_balance (assume cash available)
        self.shares_outstanding -= shares
        self.cash_balance -= redeem_value
        if self.audit_fn:
            self.audit_fn(
                "shareclass.redeem",
                {
                    "class_id": self.class_id,
                    "shares": shares,
                    "amount": redeem_value,
                    "timestamp": _now_iso(),
                },
            )
        return redeem_value

    def total_value(self) -> float:
        return float(self.cash_balance + sum(self.assets.values()))

    def compute_nav(self) -> Dict[str, float]:
        total = self.total_value()
        nav_per_share = (
            total / self.shares_outstanding if self.shares_outstanding > 0 else 0.0
        )
        return {"total": total, "nav_per_share": nav_per_share}

    def allocate_asset(self, asset_id: str, market_value: float) -> None:
        # set or update asset market value for this class only
        self.assets[asset_id] = float(market_value)
        if self.audit_fn:
            self.audit_fn(
                "shareclass.allocate",
                {
                    "class_id": self.class_id,
                    "asset_id": asset_id,
                    "value": float(market_value),
                    "timestamp": _now_iso(),
                },
            )

    def apply_management_fee(self, period_days: int) -> float:
        fee = self.total_value() * self.management_fee_annual * (period_days / 365.0)
        self.cash_balance -= fee
        if self.audit_fn:
            self.audit_fn(
                "shareclass.mgmt_fee",
                {
                    "class_id": self.class_id,
                    "fee": fee,
                    "period_days": period_days,
                    "timestamp": _now_iso(),
                },
            )
        return fee

    def apply_performance_fee(self) -> float:
        # compute gain above HWM and hurdle; simplified: take total - HWM
        total = self.total_value()
        gain = max(0.0, total - self.high_water_mark)
        perf_fee = 0.0
        if gain > 0.0 and self.performance_fee > 0.0:
            # apply hurdle: simplistic annualized check omitted; charge on gain
            perf_fee = gain * self.performance_fee
            self.cash_balance -= perf_fee
            self.high_water_mark = total - perf_fee
        if self.audit_fn:
            self.audit_fn(
                "shareclass.perf_fee",
                {"class_id": self.class_id, "fee": perf_fee, "timestamp": _now_iso()},
            )
        return perf_fee


@dataclass
class ShareClassSeries:
    fund_id: str
    classes: Dict[str, ShareClass] = field(default_factory=dict)
    audit_fn: Optional[Callable[[str, Dict[str, Any]], None]] = None

    def create_class(
        self,
        class_id: str,
        currency: str,
        launch_date: str,
        initial_shares: float = 0.0,
        initial_cash: float = 0.0,
        management_fee_annual: float = 0.0,
        performance_fee: float = 0.0,
        hurdle_rate: float = 0.0,
    ) -> None:
        if class_id in self.classes:
            raise RuntimeError("class already exists")
        sc = ShareClass(
            class_id=class_id,
            currency=currency,
            launch_date=launch_date,
            shares_outstanding=initial_shares,
            cash_balance=initial_cash,
            management_fee_annual=management_fee_annual,
            performance_fee=performance_fee,
            hurdle_rate=hurdle_rate,
            high_water_mark=initial_cash + 0.0,
            audit_fn=self.audit_fn,
        )
        self.classes[class_id] = sc
        if self.audit_fn:
            self.audit_fn(
                "series.create_class",
                {
                    "fund_id": self.fund_id,
                    "class_id": class_id,
                    "timestamp": _now_iso(),
                },
            )

    def get_class(self, class_id: str) -> ShareClass:
        return self.classes[class_id]

    def total_fund_assets(self) -> float:
        return sum([c.total_value() for c in self.classes.values()])
