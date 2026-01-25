from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping


@dataclass
class WikipediaSource:
    cfg: Mapping[str, Any]
    name: str = "wikipedia"

    def __post_init__(self) -> None:
        self.enabled = bool(self.cfg.get("enabled", False))

    def cache_key(self, *, asof: date) -> str:
        return f"{self.name}_{asof.isoformat()}"

    def fetch_raw(self, *, asof: date, allow_net: bool) -> Mapping[str, Any] | None:
        if not allow_net:
            return None
        return {"momentum": float(self.cfg.get("momentum", 0.0))}

    def normalize(self, raw: Mapping[str, Any]) -> Mapping[str, Any]:
        return raw
