from typing import Dict, Optional

from octa_strategy.lifecycle import StrategyLifecycle
from octa_strategy.state_machine import LifecycleState


class ShadowGateFailure(Exception):
    pass


class ShadowGates:
    """Gates to promote a strategy from SHADOW -> LIVE.

    Required deterministic metric keys:
      - runtime_days
      - deviation_vs_paper (abs)
      - projected_aum
      - capacity_limit
      - incidents
      - risk_budget_utilization (fraction <=1)
    """

    DEFAULT_THRESHOLDS = {
        "runtime_days": 14.0,
        "max_deviation": 0.05,
        "capacity_buffer": 1.0,  # projected_aum <= capacity_limit * buffer
        "incidents": 0,
        "risk_budget_utilization": 1.0,
    }

    def __init__(
        self, audit_fn=None, sentinel_api=None, thresholds: Optional[Dict] = None
    ):
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.sentinel_api = sentinel_api
        self.thresholds = dict(self.DEFAULT_THRESHOLDS)
        if thresholds:
            self.thresholds.update(thresholds)

    def evaluate(self, metrics: Dict) -> Dict[str, Dict]:
        t = self.thresholds
        res = {}
        res["runtime_days"] = {
            "value": float(metrics.get("runtime_days", 0.0)),
            "pass": float(metrics.get("runtime_days", 0.0)) >= t["runtime_days"],
            "threshold": t["runtime_days"],
        }
        res["deviation_vs_paper"] = {
            "value": float(metrics.get("deviation_vs_paper", 999.0)),
            "pass": float(metrics.get("deviation_vs_paper", 999.0))
            <= t["max_deviation"],
            "threshold": t["max_deviation"],
        }
        projected = float(metrics.get("projected_aum", 0.0))
        cap = float(metrics.get("capacity_limit", 0.0))
        buffer = float(t.get("capacity_buffer", 1.0))
        res["capacity"] = {
            "value": projected,
            "pass": (cap > 0 and projected <= cap * buffer),
            "threshold": cap,
        }
        res["incidents"] = {
            "value": int(metrics.get("incidents", 1)),
            "pass": int(metrics.get("incidents", 1)) <= t["incidents"],
            "threshold": t["incidents"],
        }
        res["risk_budget_utilization"] = {
            "value": float(metrics.get("risk_budget_utilization", 2.0)),
            "pass": float(metrics.get("risk_budget_utilization", 2.0))
            <= t["risk_budget_utilization"],
            "threshold": t["risk_budget_utilization"],
        }

        self.audit_fn("shadow_gates.evaluate", {"metrics": metrics, "results": res})
        return res

    def can_promote(self, metrics: Dict) -> bool:
        return all(v["pass"] for v in self.evaluate(metrics).values())

    def promote_if_pass(
        self, lifecycle: StrategyLifecycle, metrics: Dict, doc: str
    ) -> None:
        if lifecycle.current_state != LifecycleState.SHADOW:
            raise ShadowGateFailure(
                f"Strategy not in SHADOW state (current={lifecycle.current_state})"
            )

        results = self.evaluate(metrics)
        failed = {k: v for k, v in results.items() if not v["pass"]}
        if failed:
            if self.sentinel_api is not None:
                try:
                    self.sentinel_api.set_gate(
                        2, f"shadow_gate_failed:{list(failed.keys())}"
                    )
                except Exception:
                    pass
            self.audit_fn("shadow_gates.failed", {"failed": failed})
            raise ShadowGateFailure(f"Shadow gates failed: {list(failed.keys())}")

        lifecycle.transition_to(LifecycleState.LIVE, doc=doc)
        self.audit_fn(
            "shadow_gates.promoted", {"strategy_id": lifecycle.strategy_id, "doc": doc}
        )
