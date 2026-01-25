from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Protocol


@dataclass(frozen=True)
class AltDataSnapshot:
    source: str
    asof: date
    payload_path: str
    meta: Mapping[str, Any]


class AltDataSource(Protocol):
    name: str

    def cache_key(self, *, asof: date) -> str:
        ...

    def fetch_raw(self, *, asof: date, allow_net: bool) -> Mapping[str, Any] | None:
        ...

    def normalize(self, raw: Mapping[str, Any]) -> Mapping[str, Any]:
        ...
