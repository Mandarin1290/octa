import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, List


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()


@dataclass
class SimulationRun:
    id: str
    loss: float
    recovery_time: float
    incident_count: int
    gates_activated: int
    gates_success: int
    meta: Dict[str, Any]


class ScoringEngine:
    """Compute resilience metrics and a comparable resilience score across runs.

    Metrics produced:
    - max_capital_loss
    - avg_recovery_time
    - total_incidents
    - gate_success_rate

    Score: weighted aggregation emphasizing survivability (lower loss, faster recovery, fewer incidents, higher gate success).
    """

    def __init__(self, weights: Dict[str, float] | None = None):
        # default weights for loss, recovery, incidents, gate
        self.weights = weights or {
            "loss": 0.4,
            "recovery": 0.2,
            "incidents": 0.2,
            "gate": 0.2,
        }

    def compute_metrics(self, runs: List[SimulationRun]) -> Dict[str, Any]:
        # deterministic ordering
        runs_sorted = sorted(runs, key=lambda r: r.id)
        max_loss = max((r.loss for r in runs_sorted), default=0.0)
        avg_recovery = sum(r.recovery_time for r in runs_sorted) / max(
            1, len(runs_sorted)
        )
        total_incidents = sum(r.incident_count for r in runs_sorted)
        total_activated = sum(r.gates_activated for r in runs_sorted)
        total_success = sum(r.gates_success for r in runs_sorted)
        gate_rate = (total_success / total_activated) if total_activated > 0 else 1.0
        metrics: Dict[str, Any] = {
            "max_capital_loss": max_loss,
            "avg_recovery_time": avg_recovery,
            "total_incidents": total_incidents,
            "gate_success_rate": gate_rate,
        }
        metrics["hash"] = _hash(metrics)
        return metrics

    def score(self, runs: List[SimulationRun]) -> float:
        m = self.compute_metrics(runs)
        # normalize components to [0,1] where 1 is best
        loss_comp = 1.0 / (1.0 + m["max_capital_loss"])
        recovery_comp = 1.0 / (1.0 + m["avg_recovery_time"])
        incidents_comp = 1.0 / (1.0 + m["total_incidents"])
        gate_comp = m["gate_success_rate"]

        w = self.weights
        raw = (
            w["loss"] * loss_comp
            + w["recovery"] * recovery_comp
            + w["incidents"] * incidents_comp
            + w["gate"] * gate_comp
        )
        # scale to 0..100
        return round(raw * 100.0, 4)


__all__ = ["SimulationRun", "ScoringEngine"]
