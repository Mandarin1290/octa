"""Helpers for chaos drills: simple append-only audit and incident log stubs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class AuditStub:
    def __init__(self, path: str, fail: bool = False):
        self.path = Path(path)
        self.fail = fail

    def audit(self, event_type: str, payload: Any) -> None:
        if self.fail:
            raise IOError("simulated audit failure")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(
                json.dumps({"event": event_type, "payload": payload}, sort_keys=True)
                + "\n"
            )


class IncidentLog:
    def __init__(self, path: str):
        self.path = Path(path)

    def record(self, incident: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(incident, sort_keys=True) + "\n")
