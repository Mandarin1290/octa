from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from octa.core.capital.engine import CapitalDecision
from octa.core.portfolio.engine import PortfolioDecision
from octa.core.governance.audit_chain import AuditChain

from .fills import Fill
from .orders import ExecutionReport, OrderRequest, OrderStatus
from .state import ExecutionState


@dataclass
class OMSConfig:
    simulation: bool = True
    audit_path: Path | None = None
    audit_chain: AuditChain | None = None


class OMS:
    def __init__(self, config: OMSConfig | None = None) -> None:
        self._config = config or OMSConfig()
        self._state = ExecutionState()
        self._order_seq = 0

    def submit(self, portfolio_decision: PortfolioDecision) -> list[ExecutionReport]:
        reports: list[ExecutionReport] = []
        if not portfolio_decision.allow_trades:
            return reports

        requests = _build_requests(portfolio_decision.approved_trades)
        for request in requests:
            report = self._simulate_order(request)
            reports.append(report)
            self._state.reports.append(report)
            if self._config.audit_path is not None:
                _write_audit(self._config.audit_path, report)
            if self._config.audit_chain is not None:
                self._config.audit_chain.append(
                    {
                        "event": "execution_report",
                        "order_id": report.order_id,
                        "symbol": report.symbol,
                        "side": report.side,
                        "qty": report.qty,
                        "filled_qty": report.filled_qty,
                        "avg_fill_price": report.avg_fill_price,
                        "status": report.status.value,
                        "slippage": report.slippage,
                        "latency_ms": report.latency_ms,
                    }
                )
        return reports

    def state(self) -> ExecutionState:
        return self._state

    def _simulate_order(self, request: OrderRequest) -> ExecutionReport:
        self._order_seq += 1
        order_id = request.order_id or f"OMS-{self._order_seq}"
        expected = request.expected_price or request.limit_price or 0.0
        fill_price = request.limit_price if request.order_type == "LIMIT" else expected
        if fill_price is None:
            fill_price = expected
        slippage = (fill_price - expected) if expected else 0.0
        latency_ms = 0

        fill = Fill(
            order_id=order_id,
            symbol=request.symbol,
            qty=request.qty,
            price=float(fill_price),
            expected_price=expected,
            slippage=slippage,
            timestamp=datetime.utcnow(),
            latency_ms=latency_ms,
        )
        self._state.fills.append(fill)

        return ExecutionReport(
            order_id=order_id,
            symbol=request.symbol,
            side=request.side,
            qty=request.qty,
            filled_qty=request.qty,
            avg_fill_price=float(fill_price),
            status=OrderStatus.FILLED,
            slippage=slippage,
            latency_ms=latency_ms,
            fills=[{"qty": request.qty, "price": float(fill_price)}],
        )


def _build_requests(decisions: Sequence[CapitalDecision]) -> list[OrderRequest]:
    requests: list[OrderRequest] = []
    for decision in decisions:
        if not decision.allow_trade:
            continue
        if decision.symbol is None:
            continue
        plan = decision.execution_plan or {}
        if plan.get("action") != "ENTER":
            continue
        side = plan.get("side")
        if side not in {"BUY", "SELL"}:
            continue
        order_type = plan.get("order_type", "MARKET")
        limit_price = plan.get("limit_price")
        expected_price = plan.get("expected_price")
        requests.append(
            OrderRequest(
                symbol=decision.symbol,
                side=side,
                qty=decision.position_size,
                order_type=order_type,
                limit_price=limit_price,
                expected_price=expected_price,
                time_in_force=str(plan.get("time_in_force", "DAY")),
            )
        )
    return requests


def _write_audit(path: Path, report: ExecutionReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "ts": datetime.utcnow().isoformat(),
        "order_id": report.order_id,
        "symbol": report.symbol,
        "side": report.side,
        "qty": report.qty,
        "filled_qty": report.filled_qty,
        "avg_fill_price": report.avg_fill_price,
        "status": report.status.value,
        "slippage": report.slippage,
        "latency_ms": report.latency_ms,
        "fills": report.fills,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True))
        handle.write("\n")
