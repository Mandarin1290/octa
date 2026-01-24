import copy
import hashlib
import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _canonical_serialize(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _compute_hash(obj: Any) -> str:
    return hashlib.sha256(_canonical_serialize(obj).encode("utf-8")).hexdigest()


@dataclass
class SimulationContext:
    id: str
    payload: Any
    metadata: Dict[str, Any]


@dataclass
class WarGameResult:
    id: str
    scenario: str
    seed: int
    output: Any
    ts: str
    hash: str
    notes: Dict[str, Any]


class ScenarioRegistry:
    def __init__(self):
        self._registry: Dict[str, Callable[[Any, random.Random], Any]] = {}

    def register(self, name: str, fn: Callable[[Any, random.Random], Any]) -> None:
        if name in self._registry:
            raise ValueError(f"scenario already registered: {name}")
        self._registry[name] = fn

    def get(self, name: str) -> Callable[[Any, random.Random], Any]:
        return self._registry[name]


class WarGameFramework:
    """Generic war-game simulation framework.

    - Scenarios run against an isolated deep copy of provided context.
    - Deterministic via explicit RNG seed.
    - Results are auditable (timestamp, id, hash).
    """

    def __init__(self):
        self.scenarios = ScenarioRegistry()
        self.results: List[WarGameResult] = []

    def register_scenario(
        self, name: str, fn: Callable[[Any, random.Random], Any]
    ) -> None:
        self.scenarios.register(name, fn)

    def run_scenario(
        self,
        name: str,
        context_payload: Any,
        seed: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> WarGameResult:
        if seed is None:
            seed = random.Random().randint(0, 2**31 - 1)
        # isolate context
        working = copy.deepcopy(context_payload)
        ctx = SimulationContext(
            id=str(uuid.uuid4()), payload=working, metadata=dict(metadata or {})
        )

        rng = random.Random(seed)

        # run scenario function
        fn = self.scenarios.get(name)
        output = fn(ctx.payload, rng)

        ts = _utc_now_iso()
        # deterministic hash over scenario name, seed and output
        content = {"scenario": name, "seed": seed, "output": output}
        h = _compute_hash(content)
        res = WarGameResult(
            id=str(uuid.uuid4()),
            scenario=name,
            seed=seed,
            output=output,
            ts=ts,
            hash=h,
            notes={"context_id": ctx.id},
        )
        self.results.append(res)
        return res

    def replay_result(
        self, result: WarGameResult, context_payload: Any
    ) -> WarGameResult:
        # replay using recorded seed and same scenario against a fresh isolated context
        return self.run_scenario(
            result.scenario,
            context_payload,
            seed=result.seed,
            metadata={"replay_of": result.id},
        )

    def export_result_json(self, result: WarGameResult) -> str:
        return _canonical_serialize(
            {
                "id": result.id,
                "scenario": result.scenario,
                "seed": result.seed,
                "output": result.output,
                "ts": result.ts,
                "hash": result.hash,
                "notes": result.notes,
            }
        )


__all__ = ["WarGameFramework", "SimulationContext", "WarGameResult", "ScenarioRegistry"]
