import hashlib
import json
from dataclasses import asdict
from typing import Any, Dict, List

from octa_wargames.scoring import ScoringEngine, SimulationRun


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()


class WarGameDashboard:
    """Centralized war-game control and review.

    - Keeps active simulations separate from finalized runs.
    - Finalized results are stored immutably (hashed snapshot).
    - Aggregates resilience scores via `ScoringEngine`.
    """

    def __init__(self):
        self._active: Dict[str, Dict[str, Any]] = {}
        self._finalized: Dict[str, Dict[str, Any]] = {}
        self._runs: List[SimulationRun] = []
        self._scoring = ScoringEngine()

    def start_simulation(self, sim_id: str, metadata: Dict[str, Any]) -> None:
        if sim_id in self._finalized:
            raise ValueError("simulation already finalized")
        self._active[sim_id] = dict(metadata)

    def active_simulations(self) -> List[str]:
        return sorted(list(self._active.keys()))

    def finalize_simulation(self, sim_id: str, run: SimulationRun) -> Dict[str, Any]:
        if sim_id in self._finalized:
            raise ValueError("simulation already finalized")
        # freeze run into immutable snapshot
        snapshot = asdict(run)
        snapshot_hash = _hash(snapshot)
        snapshot["hash"] = snapshot_hash
        self._finalized[sim_id] = snapshot
        self._runs.append(run)
        # remove from active if present
        self._active.pop(sim_id, None)
        return snapshot

    def scenario_outcomes(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._finalized)

    def resilience_score(self) -> float:
        return self._scoring.score(self._runs)

    def uncovered_weaknesses(
        self, gate_success_threshold: float = 0.8, loss_threshold: float = 0.5
    ) -> List[Dict[str, Any]]:
        weaknesses = []
        for sim_id, snap in self._finalized.items():
            loss = snap.get("loss", 0.0)
            gates_activated = snap.get("gates_activated", 0)
            gates_success = snap.get("gates_success", 0)
            gate_rate = (
                (gates_success / gates_activated) if gates_activated > 0 else 1.0
            )
            if gate_rate < gate_success_threshold or loss > loss_threshold:
                weaknesses.append(
                    {
                        "sim_id": sim_id,
                        "loss": loss,
                        "gate_rate": gate_rate,
                        "remediation": f"https://remediation.local/{sim_id}",
                    }
                )
        return weaknesses


__all__ = ["WarGameDashboard"]
