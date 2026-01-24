from __future__ import annotations

from dataclasses import dataclass, field

from .fills import Fill
from .orders import ExecutionReport, OrderRequest


@dataclass
class ExecutionState:
    open_orders: dict[str, OrderRequest] = field(default_factory=dict)
    fills: list[Fill] = field(default_factory=list)
    reports: list[ExecutionReport] = field(default_factory=list)
