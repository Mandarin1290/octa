from typing import Dict, Optional

from octa_strategy.lifecycle import StrategyLifecycle


class KillActionError(Exception):
    pass


class KillRules:
    """Automatic suspension and retirement rules for strategies.

    Configurable triggers and repetition counters allow for persistent detection.
    Manual overrides require committee approval (passed as flag) and are audited.
    """

    def __init__(
        self,
        audit_fn=None,
        sentinel_api=None,
        underperf_threshold: float = 0.2,
        underperf_repeat: int = 3,
        retire_repeat: int = 5,
        incidents_threshold: int = 2,
        retire_on_alpha_decay: bool = False,
    ):
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.sentinel_api = sentinel_api
        self.underperf_threshold = underperf_threshold
        self.underperf_repeat = underperf_repeat
        self.retire_repeat = retire_repeat
        self.incidents_threshold = incidents_threshold
        self.retire_on_alpha_decay = retire_on_alpha_decay

        # counters
        self._underperf_counts: Dict[str, int] = {}
        self._incident_counts: Dict[str, int] = {}

    def _audit(self, evt: str, payload: Dict):
        self.audit_fn(evt, payload)

    def evaluate_triggers(self, strategy_id: str, metrics: Dict) -> Dict[str, bool]:
        """Return which triggers are True for provided metrics.

        Expected metric keys (deterministic):
          - drawdown: positive float
          - alpha_decay: positive float (abs diff)
          - incidents: int
          - structural_risk: bool
        """
        drawdown = float(metrics.get("drawdown", 0.0))
        alpha_decay = float(metrics.get("alpha_decay", 0.0))
        incidents = int(metrics.get("incidents", 0))
        structural = bool(metrics.get("structural_risk", False))

        underperf = drawdown >= self.underperf_threshold
        alpha_flag = self.retire_on_alpha_decay and alpha_decay > 0
        incidents_flag = incidents >= self.incidents_threshold

        return {
            "underperf": underperf,
            "alpha_decay": alpha_flag,
            "incidents": incidents_flag,
            "structural": structural,
        }

    def process(
        self,
        strategy_id: str,
        lifecycle: StrategyLifecycle,
        metrics: Dict,
        doc: Optional[str] = None,
    ) -> None:
        """Evaluate triggers and enforce suspension or retirement as needed.

        The logic is:
        - structural risk -> immediate suspension (and audit)
        - underperformance increments counter; when >= underperf_repeat -> suspend
        - if underperf counter >= retire_repeat -> retire
        - incidents increment counter and may lead to suspend/retire similarly
        """
        triggers = self.evaluate_triggers(strategy_id, metrics)
        self._audit(
            "kill.evaluate",
            {"strategy_id": strategy_id, "metrics": metrics, "triggers": triggers},
        )

        # structural risk immediate suspend
        if triggers.get("structural"):
            try:
                lifecycle.transition_to(
                    "SUSPENDED", doc=(doc or "structural risk auto-suspend")
                )
            except Exception:
                pass
            try:
                if self.sentinel_api is not None:
                    self.sentinel_api.set_gate(3, f"structural_risk:{strategy_id}")
            except Exception:
                pass
            self._audit(
                "kill.enforce",
                {
                    "strategy_id": strategy_id,
                    "action": "suspend",
                    "reason": "structural",
                },
            )
            return

        # underperformance handling
        if triggers.get("underperf"):
            self._underperf_counts[strategy_id] = (
                self._underperf_counts.get(strategy_id, 0) + 1
            )
        else:
            self._underperf_counts[strategy_id] = 0

        # incidents
        if triggers.get("incidents"):
            self._incident_counts[strategy_id] = (
                self._incident_counts.get(strategy_id, 0) + 1
            )
        else:
            self._incident_counts[strategy_id] = 0

        upc = self._underperf_counts.get(strategy_id, 0)
        ic = self._incident_counts.get(strategy_id, 0)

        # retirement due to persistent underperformance
        if upc >= self.retire_repeat:
            try:
                lifecycle.transition_to(
                    "RETIRED", doc=(doc or "auto-retire persistent underperformance")
                )
            except Exception:
                pass
            try:
                if self.sentinel_api is not None:
                    self.sentinel_api.set_gate(3, f"auto_retire:{strategy_id}")
            except Exception:
                pass
            self._audit(
                "kill.enforce",
                {
                    "strategy_id": strategy_id,
                    "action": "retire",
                    "reason": "persistent_underperf",
                    "count": upc,
                },
            )
            return

        # retirement due to incidents
        if ic >= self.retire_repeat:
            try:
                lifecycle.transition_to(
                    "RETIRED", doc=(doc or "auto-retire persistent incidents")
                )
            except Exception:
                pass
            try:
                if self.sentinel_api is not None:
                    self.sentinel_api.set_gate(
                        3, f"auto_retire_incidents:{strategy_id}"
                    )
            except Exception:
                pass
            self._audit(
                "kill.enforce",
                {
                    "strategy_id": strategy_id,
                    "action": "retire",
                    "reason": "persistent_incidents",
                    "count": ic,
                },
            )
            return

        # suspension due to persistent underperformance or incidents
        if upc >= self.underperf_repeat or ic >= self.underperf_repeat:
            try:
                lifecycle.transition_to(
                    "SUSPENDED", doc=(doc or "auto-suspend persistent issues")
                )
            except Exception:
                pass
            try:
                if self.sentinel_api is not None:
                    self.sentinel_api.set_gate(2, f"auto_suspend:{strategy_id}")
            except Exception:
                pass
            self._audit(
                "kill.enforce",
                {
                    "strategy_id": strategy_id,
                    "action": "suspend",
                    "reason": "persistent_issues",
                    "upc": upc,
                    "ic": ic,
                },
            )
            return

    def manual_retire(
        self,
        strategy_id: str,
        lifecycle: StrategyLifecycle,
        committee_approved: bool,
        doc: Optional[str] = None,
    ) -> None:
        """Manually retire a strategy only if `committee_approved` is True. Otherwise raises."""
        if not committee_approved:
            raise KillActionError("Committee approval required for manual retirement")
        lifecycle.transition_to("RETIRED", doc=(doc or "manual retire by committee"))
        self._audit("kill.manual_retire", {"strategy_id": strategy_id, "doc": doc})
