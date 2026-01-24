from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from octa_core.ids import generate_id
from octa_vertex.models import ExecutionReport, Order, OrderStatus
from octa_vertex.slippage import pre_trade_slippage_estimate


class PaperExecutor:
    """Simulate market-aware VWAP-style paper execution.

    Bars: list of dicts with keys: `vwap` (price), `volume` (bar volume)
    participation: fraction of bar volume (0..1)
    """

    def __init__(
        self,
        participation: float = 0.1,
        ledger_api: Optional[Any] = None,
        sentinel: Optional[Any] = None,
    ):
        self.participation = float(participation)
        self.ledger: Optional[Any] = ledger_api
        self.sentinel: Optional[Any] = sentinel

    def execute(
        self,
        order: Order,
        bars: List[Dict],
        adv: float,
        sigma: float,
        half_spread: float,
        impact_coeff: float = 0.1,
        fixed_fees: float = 0.0,
        ts_field: str | None = None,
    ) -> List[ExecutionReport]:
        remaining = order.qty
        reports: List[ExecutionReport] = []

        for _i, bar in enumerate(bars):
            # check sentinel before each slice
            if self.sentinel is not None:
                try:
                    dec = self.sentinel.evaluate({})
                    lvl = getattr(dec, "level", 0)
                    if lvl >= 2:
                        # publish incident via ledger and stop
                        if self.ledger is not None:
                            self.ledger.audit_or_fail(
                                "executor",
                                "execution_halted",
                                {
                                    "order_id": str(order.id),
                                    "reason": "sentinel_freeze",
                                },
                            )
                        break
                except Exception:
                    # on sentinel errors be conservative and stop
                    if self.ledger is not None:
                        self.ledger.audit_or_fail(
                            "executor",
                            "execution_halted",
                            {"order_id": str(order.id), "reason": "sentinel_error"},
                        )
                    break

            bar_vol = float(bar.get("volume", 0.0))
            bar_price = float(bar.get("vwap", 0.0))
            max_part = self.participation * bar_vol
            slice_size = min(remaining, max_part)
            if slice_size <= 0:
                continue

            ts = bar.get("ts") if ts_field is None else bar.get(ts_field)
            # estimate slippage and cost
            est = pre_trade_slippage_estimate(
                size=slice_size,
                price=bar_price,
                adv=adv,
                sigma=sigma,
                half_spread=half_spread,
                ts=ts or time.strftime("%Y-%m-%dT%H:%M:%S"),
            )

            # compute executed price: assume impact pushes price by est[impact_pct]
            impact_pct = est.get("impact_pct", 0.0)
            if order.side.value == "BUY":
                exec_price = bar_price * (1.0 + impact_pct)
            else:
                exec_price = bar_price * (1.0 - impact_pct)

            filled = slice_size
            remaining -= filled

            # record audit
            if self.ledger is not None:
                self.ledger.audit_or_fail(
                    "executor",
                    "order_fill",
                    {
                        "order_id": str(order.id),
                        "filled": filled,
                        "price": exec_price,
                        "cost_estimate": est,
                    },
                )

            report = ExecutionReport(
                order_id=order.id,
                report_id=str(generate_id("rep")),
                status=OrderStatus.PARTIAL if remaining > 0 else OrderStatus.FILLED,
                filled_qty=filled,
                remaining_qty=remaining,
                msg=None,
            )
            reports.append(report)

            if remaining <= 1e-9:
                break

        # if any remaining, mark as partial/unfilled
        if remaining > 1e-9 and self.ledger is not None:
            self.ledger.audit_or_fail(
                "executor",
                "order_unfilled",
                {"order_id": str(order.id), "remaining": remaining},
            )

        return reports
