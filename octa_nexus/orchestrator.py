from __future__ import annotations

from octa_atlas.registry import AtlasRegistry as Registry
from octa_ledger.core import AuditChain
from octa_vertex.orders import ExecutionEngine


class WiringError(Exception):
    pass


class Orchestrator:
    def __init__(
        self, registry: Registry, ledger: AuditChain, engine: ExecutionEngine
    ) -> None:
        self.registry = registry
        self.ledger = ledger
        self.engine = engine

    def validate(self) -> None:
        # Ensure minimal invariants hold
        if self.registry is None:
            raise WiringError("registry missing")
        if self.ledger is None:
            raise WiringError("ledger missing")
        if self.engine is None:
            raise WiringError("execution engine missing")

    def submit_order(self, order) -> None:
        # Record submission intent
        self.ledger.append({"action": "submit_order", "order_id": str(order.id)})
        # delegate execution
        res = self.engine.execute(order)
        self.ledger.append(
            {
                "action": "order_executed",
                "order_id": str(res.id),
                "status": res.status.value,
            }
        )
