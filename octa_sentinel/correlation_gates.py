"""Correlation gates for Sentinel integration.

This module provides a small adapter that translates correlation detector outputs
into sentinel gate actions and allocator downscaling recommendations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict


@dataclass
class CorrelationGates:
    avg_threshold: float = 0.35
    score_threshold_warn: float = 0.4
    score_threshold_derisk: float = 0.7

    def evaluate_and_act(
        self,
        result: Dict[str, Any],
        sentinel_api: Any | None = None,
        allocator_api: Any | None = None,
        audit_fn: Callable[[str, Dict], None] | None = None,
    ) -> Dict[str, Any]:
        """Evaluate detector `result` and optionally call sentinel/allocator APIs.

        sentinel_api expected API: `set_gate(level: int, reason: str)` where level 0-3.
        allocator_api expected API: `scale_risk(factor: float)` applying compression.
        audit_fn expected API: `audit_fn(event_type, payload)` for ledger/audit.
        """
        score = float(result.get("score", 0.0))
        metrics = result.get("metrics", {})
        recommended = float(result.get("recommended_compression", 1.0))

        action: Dict[str, Any] = {
            "score": score,
            "recommended_compression": recommended,
        }

        # Determine gate level
        if score >= self.score_threshold_derisk:
            level = 3
            reason = "correlation_breakdown:derisk"
        elif score >= self.score_threshold_warn:
            level = 2
            reason = "correlation_breakdown:warning"
        else:
            level = 0
            reason = "correlation_breakdown:ok"

        action["gate_level"] = level
        action["reason"] = reason

        if audit_fn is not None:
            try:
                audit_fn(
                    "correlation_gate.evaluation",
                    {"score": score, "metrics": metrics, "reason": reason},
                )
            except Exception:
                # Fail-closed policy could escalate here; keep local behavior minimal.
                pass

        if sentinel_api is not None:
            try:
                sentinel_api.set_gate(level, reason)
            except Exception:
                pass

        if allocator_api is not None and level > 0:
            try:
                allocator_api.scale_risk(recommended)
            except Exception:
                pass

        return action
