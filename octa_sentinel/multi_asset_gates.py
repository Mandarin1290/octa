from typing import Any, Dict


class MultiAssetGates:
    """Per-asset-class readiness gates. Each gate is independent.

    Expected input `status` is a mapping per asset class with gate-specific indicators, for example:
      {
        'futures': {'roll_tested': True},
        'fx': {'funding_ratio': 0.95},
        'rates': {'stress_passed': True},
        'vol': {'exposure': 0.5, 'exposure_cap': 1.0},
        'commodities': {'delivery_guard': True}
      }

    On failure the engine calls `sentinel_api.set_gate(level, reason)` for that asset class only.
    """

    def __init__(
        self, sentinel_api=None, audit_fn=None, fx_funding_threshold: float = 0.8
    ):
        self.sentinel_api = sentinel_api
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.fx_funding_threshold = fx_funding_threshold

    def evaluate_gate_for_asset(
        self, asset: str, info: Dict[str, Any]
    ) -> Dict[str, Any]:
        ok = True
        reason = ""

        if asset == "futures":
            if not info.get("roll_tested", False):
                ok = False
                reason = "futures_roll_not_tested"

        elif asset == "fx":
            fr = float(info.get("funding_ratio", 0.0))
            if fr < self.fx_funding_threshold:
                ok = False
                reason = f"fx_funding_unstable:{fr:.2f}"

        elif asset == "rates":
            if not info.get("stress_passed", False):
                ok = False
                reason = "rates_stress_failed"

        elif asset == "vol":
            exposure = float(info.get("exposure", 0.0))
            cap = float(info.get("exposure_cap", 1.0))
            if exposure > cap:
                ok = False
                reason = f"vol_exposure_exceeded:{exposure:.3f}>{cap:.3f}"

        elif asset == "commodities":
            if not info.get("delivery_guard", False):
                ok = False
                reason = "commodity_delivery_guard_failed"

        else:
            # unknown asset: default to not ready
            ok = False
            reason = "unknown_asset_gate"

        if not ok:
            try:
                if self.sentinel_api is not None:
                    self.sentinel_api.set_gate(3, f"gate_fail:{asset}:{reason}")
            except Exception:
                pass
            self.audit_fn("gate_fail", {"asset": asset, "reason": reason})
        else:
            self.audit_fn("gate_pass", {"asset": asset})

        return {"asset": asset, "ok": ok, "reason": reason}

    def evaluate_all(
        self, status: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        report: Dict[str, Dict[str, Any]] = {}
        for asset, info in status.items():
            report[asset] = self.evaluate_gate_for_asset(asset, info)
        return report
