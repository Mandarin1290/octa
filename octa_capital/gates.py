import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict


def canonical_hash(obj) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class GateError(Exception):
    pass


class GateClosed(GateError):
    pass


@dataclass
class GateRecord:
    timestamp: str
    action: str
    details: Dict[str, Any]
    evidence_hash: str = ""


class CapitalGates:
    """Capital protection gates: redemption gates, side-pockets, stress limits.

    Controls override investor requests via `evaluate_redemption`.
    """

    def __init__(
        self,
        base_limit_percent: float = 0.2,
        survival_ratio: float = 0.05,
        stress_sensitivity: float = 0.9,
    ):
        self.redemption_open = True
        self.side_pocket_enabled = False
        self.side_pocket_allocation = 0.0
        self.base_limit_percent = float(base_limit_percent)
        self.survival_ratio = float(survival_ratio)
        self.stress_metric = 0.0
        self.stress_sensitivity = float(stress_sensitivity)
        self.audit_log: list[GateRecord] = []

    def _now_iso(self):
        return datetime.now(timezone.utc).isoformat()

    def _record(self, action: str, details: Dict[str, Any]):
        ts = self._now_iso()
        rec = GateRecord(timestamp=ts, action=action, details=details)
        rec.evidence_hash = canonical_hash(
            {"ts": ts, "action": action, "details": details}
        )
        self.audit_log.append(rec)

    def set_redemption_gate(self, open: bool):
        self.redemption_open = bool(open)
        self._record("set_redemption_gate", {"open": self.redemption_open})

    def enable_side_pocket(self, allocation_percent: float):
        if not (0.0 <= allocation_percent <= 1.0):
            raise ValueError("allocation_percent must be between 0 and 1")
        self.side_pocket_enabled = True
        self.side_pocket_allocation = float(allocation_percent)
        self._record(
            "enable_side_pocket", {"allocation_percent": self.side_pocket_allocation}
        )

    def disable_side_pocket(self):
        self.side_pocket_enabled = False
        self.side_pocket_allocation = 0.0
        self._record("disable_side_pocket", {})

    def set_stress_metric(self, val: float):
        if not (0.0 <= val <= 1.0):
            raise ValueError("stress metric must be in [0,1]")
        self.stress_metric = float(val)
        self._record("set_stress_metric", {"stress_metric": self.stress_metric})

    def evaluate_redemption(
        self,
        requested_value: float,
        investor_balance: float,
        liquid_assets: float,
        portfolio_value: float,
    ) -> Dict[str, Any]:
        """Evaluate and potentially override a redemption request.

        Returns a dict with keys: allowed (float), side_pocket (float), blocked (bool), reason (str).
        """
        details: Dict[str, Any] = {
            "requested_value": requested_value,
            "investor_balance": investor_balance,
            "liquid_assets": liquid_assets,
            "portfolio_value": portfolio_value,
            "redemption_open": self.redemption_open,
            "stress_metric": self.stress_metric,
        }

        if not self.redemption_open:
            self._record("evaluate_redemption_blocked", details)
            return {
                "allowed": 0.0,
                "side_pocket": 0.0,
                "blocked": True,
                "reason": "gate_closed",
            }

        # Compute survival buffer: keep survival_ratio * portfolio_value in liquid assets
        survival_buffer = self.survival_ratio * float(portfolio_value)
        max_available = max(0.0, float(liquid_assets) - survival_buffer)

        # Stress-adjusted percent cap of portfolio
        cap_percent = self.base_limit_percent * (
            1.0 - (self.stress_metric * self.stress_sensitivity)
        )
        cap_percent = max(0.0, cap_percent)
        cap_by_stress = cap_percent * float(portfolio_value)

        allowed = min(requested_value, investor_balance, max_available, cap_by_stress)

        side_pocket_amt = 0.0
        if self.side_pocket_enabled and allowed > 0.0:
            side_pocket_amt = allowed * float(self.side_pocket_allocation)
            allowed = allowed - side_pocket_amt

        reason = "allowed"
        if allowed <= 0.0:
            reason = "no_liquidity_or_limits"

        details.update(
            {"allowed": allowed, "side_pocket": side_pocket_amt, "reason": reason}
        )
        self._record("evaluate_redemption", details)
        return {
            "allowed": allowed,
            "side_pocket": side_pocket_amt,
            "blocked": False,
            "reason": reason,
        }
