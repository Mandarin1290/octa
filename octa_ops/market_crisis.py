from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class MarketCrisisManager:
    portfolio: Any
    audit_log: List[Dict[str, Any]] = field(default_factory=list)
    volatility_threshold: float = 0.05
    correlation_threshold: float = 0.7
    liquidity_threshold: float = 0.2
    base_reduction: float = 0.5

    killed: bool = False
    override_actor: Optional[str] = None
    override_ts: Optional[str] = None

    def _log(
        self, action: str, details: Dict[str, Any], actor: Optional[str] = None
    ) -> None:
        entry = {
            "ts": _utc_now_iso(),
            "actor": actor,
            "action": action,
            "details": details,
        }
        self.audit_log.append(entry)

    def evaluate(
        self, metrics: Dict[str, float], actor: Optional[str] = None
    ) -> Dict[str, Any]:
        """Evaluate market metrics and apply mitigation if triggers fire.

        metrics keys: `volatility`, `correlation`, `liquidity`.
        Returns a dict with `triggered` (bool) and `reduction_applied` (0..1).
        """
        if self.killed and not self.override_actor:
            self._log("evaluate_skipped", {"reason": "kill_switch_active"}, actor)
            return {"triggered": False, "reduction_applied": 0.0}

        vol = float(metrics.get("volatility", 0.0))
        corr = float(metrics.get("correlation", 0.0))
        liq = float(metrics.get("liquidity", 1.0))

        triggered = False
        reduction = 0.0

        if vol >= self.volatility_threshold:
            triggered = True
        if corr >= self.correlation_threshold:
            triggered = True
        if liq <= self.liquidity_threshold:
            triggered = True

        if not triggered:
            self._log("evaluate_no_trigger", {"metrics": metrics}, actor)
            return {"triggered": False, "reduction_applied": 0.0}

        # Compute conservative reduction: base reduction increased if liquidity poor
        reduction = float(self.base_reduction)
        if liq <= self.liquidity_threshold:
            reduction = min(1.0, reduction * 1.5)

        # Ensure risk reduction is prioritized: always reduce to at least the computed target
        try:
            applied = 0.0
            if hasattr(self.portfolio, "reduce_exposure"):
                applied = float(
                    self.portfolio.reduce_exposure(
                        reduction,
                        pessimistic_liquidity=(liq <= self.liquidity_threshold),
                    )
                )
            else:
                # best-effort: if portfolio exposes numeric total, scale it down
                if hasattr(self.portfolio, "scale_down"):
                    applied = float(self.portfolio.scale_down(reduction))

            self._log(
                "mitigation_applied",
                {"metrics": metrics, "target_reduction": reduction, "applied": applied},
                actor,
            )
            return {"triggered": True, "reduction_applied": applied}
        except Exception as e:
            # Fail-closed: if mitigation cannot be applied, escalate and engage kill-switch
            self._log("mitigation_failed", {"metrics": metrics, "error": str(e)}, actor)
            self.kill_switch(actor=actor)
            return {"triggered": True, "reduction_applied": 0.0}

    def kill_switch(self, actor: Optional[str] = None) -> None:
        """Activate an automated kill-switch that prevents further automated mitigations.

        Human operators may later override via `override_kill_switch` (logged).
        """
        self.killed = True
        self.override_actor = None
        self.override_ts = None
        self._log("kill_switch_activated", {}, actor)

    def override_kill_switch(self, actor: str) -> None:
        """Allow a named human actor to temporarily override the kill-switch. This is logged."""
        if not actor:
            raise ValueError("actor required for override")
        self.override_actor = actor
        self.override_ts = _utc_now_iso()
        self._log("kill_switch_overridden", {"by": actor}, actor)

    def clear_override(self, actor: Optional[str] = None) -> None:
        """Clear any human override. If `actor` provided, it is recorded in the log."""
        prev = self.override_actor
        self.override_actor = None
        self.override_ts = None
        self._log(
            "override_cleared", {"cleared_by": actor, "previous_override": prev}, actor
        )

    def allow_trade(self) -> bool:
        """Return whether automated trading is allowed under current crisis state."""
        if self.killed and not self.override_actor:
            return False
        return True
