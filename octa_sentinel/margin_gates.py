"""Margin gates to integrate margin outputs with Sentinel actions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class MarginGates:
    warn_threshold: float = 0.6
    freeze_threshold: float = 0.9

    def evaluate_and_act(
        self,
        margin_result: Dict,
        sentinel_api: Any | None = None,
        allocator_api: Any | None = None,
        audit_fn: Any | None = None,
    ) -> Dict:
        util = float(margin_result.get("margin_utilization", 0.0))
        action: Dict[str, Any] = {"utilization": util}
        level = 0
        reason = "ok"
        if util >= self.freeze_threshold or margin_result.get("breach_flags", {}).get(
            "initial_margin_breached"
        ):
            level = 3
            reason = "margin_breach"
        elif util >= self.warn_threshold:
            level = 2
            reason = "margin_warn"

        action.update({"gate_level": level, "reason": reason})

        if audit_fn:
            try:
                audit_fn("margin.evaluation", {"utilization": util, "reason": reason})
            except Exception:
                pass

        if sentinel_api:
            try:
                sentinel_api.set_gate(level, reason)
            except Exception:
                pass

        if allocator_api and level > 0:
            try:
                # scale down proportionally to utilization beyond warn threshold
                if util >= self.freeze_threshold:
                    factor = 0.0
                else:
                    factor = max(
                        0.0,
                        1.0
                        - (util - self.warn_threshold)
                        / (self.freeze_threshold - self.warn_threshold),
                    )
                allocator_api.scale_risk(factor)
                action["applied_scale"] = factor
            except Exception:
                pass

        return action
