from __future__ import annotations

from dataclasses import dataclass

from ..fills import Fill


@dataclass(frozen=True)
class IBKRFills:
    raw_fills: list[object]
    parsed: list[Fill]
