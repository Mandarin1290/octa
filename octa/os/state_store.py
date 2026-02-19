from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .utils import append_jsonl, atomic_write_json, load_json, stable_sha256, utc_now_iso


@dataclass(frozen=True)
class StatePaths:
    var_root: Path
    state_root: Path
    evidence_root: Path
    registry_path: Path
    chain_path: Path
    intents_dir: Path
    approvals_dir: Path
    blessed_registry: Path
    pid_path: Path


class OSStateStore:
    def __init__(self, var_root: Path = Path("octa") / "var") -> None:
        state_root = var_root / "state"
        self.paths = StatePaths(
            var_root=var_root,
            state_root=state_root,
            evidence_root=var_root / "evidence",
            registry_path=state_root / "os_registry.json",
            chain_path=state_root / "os_chain.jsonl",
            intents_dir=state_root / "order_intents",
            approvals_dir=state_root / "order_approvals",
            blessed_registry=state_root / "blessed_models.jsonl",
            pid_path=state_root / "os_brain.pid",
        )
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        self.paths.state_root.mkdir(parents=True, exist_ok=True)
        self.paths.evidence_root.mkdir(parents=True, exist_ok=True)
        self.paths.intents_dir.mkdir(parents=True, exist_ok=True)
        self.paths.approvals_dir.mkdir(parents=True, exist_ok=True)

    def load_registry(self) -> dict[str, Any]:
        return load_json(self.paths.registry_path, default={})

    def save_registry(self, payload: dict[str, Any]) -> None:
        atomic_write_json(self.paths.registry_path, payload)

    def append_chain(self, payload: dict[str, Any]) -> dict[str, Any]:
        registry = self.load_registry()
        prev_hash = str(registry.get("chain_head_hash", "GENESIS"))
        index = int(registry.get("chain_last_index", 0)) + 1
        node = {
            "index": index,
            "ts_utc": utc_now_iso(),
            "prev_hash": prev_hash,
            "payload": payload,
        }
        node["hash"] = stable_sha256(node)
        append_jsonl(self.paths.chain_path, node)
        registry["chain_last_index"] = index
        registry["chain_head_hash"] = node["hash"]
        self.save_registry(registry)
        return node

    def write_intent(self, order_id: str, payload: dict[str, Any]) -> Path:
        path = self.paths.intents_dir / f"{order_id}.json"
        atomic_write_json(path, payload)
        return path

    def write_approval(self, order_id: str, payload: dict[str, Any]) -> Path:
        path = self.paths.approvals_dir / f"{order_id}.json"
        atomic_write_json(path, payload)
        return path

    def read_intent(self, order_id: str) -> dict[str, Any]:
        return load_json(self.paths.intents_dir / f"{order_id}.json", default={})

    def read_approval(self, order_id: str) -> dict[str, Any]:
        return load_json(self.paths.approvals_dir / f"{order_id}.json", default={})
