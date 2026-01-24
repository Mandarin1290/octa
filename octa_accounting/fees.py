import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List


def canonical_hash(obj) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class FeeRecord:
    timestamp: str
    share_class: str
    action: str
    details: Dict
    evidence_hash: str = ""


@dataclass
class FeeBucket:
    share_class: str
    hwm: float  # High-water-mark (NAV per share)
    mgmt_rate_annual: float  # e.g. 0.02 for 2% p.a.
    perf_rate: float  # e.g. 0.20 for 20% performance fee
    accrued_mgmt: float = 0.0
    accrued_perf: float = 0.0
    payable: float = 0.0
    audit_log: List[FeeRecord] = field(default_factory=list)

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def record(self, action: str, details: Dict):
        ts = self._now_iso()
        rec = FeeRecord(
            timestamp=ts, share_class=self.share_class, action=action, details=details
        )
        rec.evidence_hash = canonical_hash(
            {
                "ts": ts,
                "share_class": self.share_class,
                "action": action,
                "details": details,
            }
        )
        self.audit_log.append(rec)

    def accrue_management(self, nav_per_share: float, days: float) -> float:
        if days < 0:
            raise ValueError("days must be non-negative")
        # Management fees accrue on assets under management (NAV per share basis assumed)
        # pro-rated: mgmt_rate_annual * (days / 365) * nav_per_share
        amt = self.mgmt_rate_annual * (days / 365.0) * nav_per_share
        self.accrued_mgmt += amt
        self.record(
            "accrue_management",
            {
                "days": days,
                "nav_per_share": nav_per_share,
                "amount": amt,
                "accrued_mgmt": self.accrued_mgmt,
            },
        )
        return amt

    def accrue_performance(self, nav_per_share: float) -> float:
        # Performance applies only on gains above HWM
        if nav_per_share <= self.hwm:
            self.record(
                "accrue_performance",
                {"nav_per_share": nav_per_share, "amount": 0.0, "hwm": self.hwm},
            )
            return 0.0
        gain = nav_per_share - self.hwm
        amt = gain * self.perf_rate
        self.accrued_perf += amt
        self.record(
            "accrue_performance",
            {
                "nav_per_share": nav_per_share,
                "hwm": self.hwm,
                "gain": gain,
                "amount": amt,
                "accrued_perf": self.accrued_perf,
            },
        )
        return amt

    def crystallize(self, nav_per_share: float) -> Dict[str, float]:
        # Crystallize accrued fees: move accrued to payable and update HWM if NAV exceeds it
        total = 0.0
        if self.accrued_mgmt:
            total += self.accrued_mgmt
            self.payable += self.accrued_mgmt
            self.record("crystallize_management", {"amount": self.accrued_mgmt})
            self.accrued_mgmt = 0.0
        if self.accrued_perf:
            total += self.accrued_perf
            self.payable += self.accrued_perf
            # On crystallization of performance fees, HWM steps up to current NAV
            self.record(
                "crystallize_performance",
                {
                    "amount": self.accrued_perf,
                    "old_hwm": self.hwm,
                    "new_hwm": nav_per_share,
                },
            )
            self.hwm = nav_per_share
            self.accrued_perf = 0.0
        self.record(
            "crystallize_total",
            {"total": total, "post_hwm": self.hwm, "payable": self.payable},
        )
        return {"total_crystallized": total, "payable": self.payable, "hwm": self.hwm}


class FeeEngine:
    """Manages fees for multiple share classes.

    Hard rules enforced:
    - Fees never drive trading decisions (engine only observes NAVs and returns fee amounts).
    - High-water-mark (HWM) strictly enforced: performance fees only on NAV above HWM.
    - Crystallization moves accrued fees to payable and updates HWM on performance crystallization.
    """

    def __init__(self):
        self.buckets: Dict[str, FeeBucket] = {}

    def add_share_class(
        self,
        share_class: str,
        initial_hwm: float,
        mgmt_rate_annual: float,
        perf_rate: float,
    ):
        if share_class in self.buckets:
            raise ValueError("share_class already exists")
        fb = FeeBucket(
            share_class=share_class,
            hwm=initial_hwm,
            mgmt_rate_annual=mgmt_rate_annual,
            perf_rate=perf_rate,
        )
        self.buckets[share_class] = fb

    def accrue_management(
        self, share_class: str, nav_per_share: float, days: float
    ) -> float:
        fb = self.buckets[share_class]
        return fb.accrue_management(nav_per_share, days)

    def accrue_performance(self, share_class: str, nav_per_share: float) -> float:
        fb = self.buckets[share_class]
        return fb.accrue_performance(nav_per_share)

    def crystallize(self, share_class: str, nav_per_share: float) -> Dict[str, float]:
        fb = self.buckets[share_class]
        return fb.crystallize(nav_per_share)

    def payable(self, share_class: str) -> float:
        return self.buckets[share_class].payable

    def snapshot_audit(self) -> Dict:
        out = {}
        for k, v in self.buckets.items():
            out[k] = {
                "hwm": v.hwm,
                "mgmt_rate_annual": v.mgmt_rate_annual,
                "perf_rate": v.perf_rate,
                "accrued_mgmt": v.accrued_mgmt,
                "accrued_perf": v.accrued_perf,
                "payable": v.payable,
                "audit_count": len(v.audit_log),
            }
        return out
