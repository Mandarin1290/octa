from __future__ import annotations

from typing import Iterable

from .models import ExecutionReport, Order
from .state_machine import OrderStateMachine


class Simulator:
    def __init__(self, oms: OrderStateMachine):
        self.oms = oms

    def replay(self, order: Order, reports: Iterable[ExecutionReport]) -> Order:
        for r in reports:
            # apply report and transition to PARTIAL or FILLED
            if r.filled_qty >= order.qty:
                order = self.oms.transition(
                    order,
                    _order_status := (
                        order.__class__.status.__class__.PARTIAL
                        if r.filled_qty < order.qty
                        else order.__class__.status.__class__.FILLED
                    ),
                    report=r,
                )
            else:
                order = self.oms.transition(
                    order, order.__class__.status.__class__.PARTIAL, report=r
                )
        return order
