from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from octa.support.branding import run_identity_payload

from .utils import atomic_write_json, stable_sha256


@dataclass(frozen=True)
class TickEvidence:
    path: Path
    payload_hash: str


class EvidenceWriter:
    def __init__(self, evidence_root: Path, run_id: str) -> None:
        self.run_id = run_id
        self.run_dir = evidence_root / run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.run_dir / "run_identity.json", run_identity_payload())

    def write_tick(self, ts_compact: str, payload: dict[str, Any]) -> TickEvidence:
        path = self.run_dir / f"os_tick_{ts_compact}.json"
        atomic_write_json(path, payload)
        return TickEvidence(path=path, payload_hash=stable_sha256(payload))
