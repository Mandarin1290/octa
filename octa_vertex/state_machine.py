from __future__ import annotations

from typing import Optional

from octa_ledger.api import LedgerAPI

from .models import ExecutionReport, Order, OrderStatus


class OrderStateMachine:
    def __init__(self, ledger: LedgerAPI):
        self.ledger = ledger

    def transition(
        self,
        order: Order,
        to_status: OrderStatus,
        report: Optional[ExecutionReport] = None,
    ) -> Order:
        # audit intent before change
        self.ledger.audit_or_fail(
            "vertex",
            "order_transition",
            {
                "order_id": str(order.id),
                "from": order.status.value,
                "to": to_status.value,
            },
        )

        order.status = to_status
        if report:
            # apply filled qty
            order.filled_qty = min(order.qty, order.filled_qty + report.filled_qty)
        # audit after
        self.ledger.audit_or_fail(
            "vertex",
            "order_transition_after",
            {
                "order_id": str(order.id),
                "status": order.status.value,
                "filled_qty": order.filled_qty,
            },
        )
        return order
