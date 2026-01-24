"""Ledger helper for persisting exposure graph snapshots.

Provides a tiny append-only snapshot writer for exposure graphs.
"""

from __future__ import annotations

import json
from datetime import datetime


class ExposureLedger:
    def __init__(self, path: str):
        self.path = path

    def append_snapshot(self, graph_serialized: str, meta: dict | None = None) -> None:
        payload = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "graph": graph_serialized,
            "meta": meta or {},
        }
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, sort_keys=True) + "\n")

    def load_latest(self) -> dict | None:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                lines = f.read().strip().splitlines()
                if not lines:
                    return None
                return json.loads(lines[-1])
        except FileNotFoundError:
            return None
