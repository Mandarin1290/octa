from __future__ import annotations

import logging
from typing import Any, Mapping, Sequence

from .context import CascadeContext
from .contracts import GateDecision, GateInterface
from .policies import CascadePolicy
from .registry import build_default_gate_stack
from .routing import route_symbols
from octa.core.capital.engine import CapitalEngine
from octa.core.capital.state import CapitalState
from octa.core.risk.allrad.engine import ALLRADEngine
from octa.core.risk.allrad.engine import RiskDecision

logger = logging.getLogger(__name__)


class CascadeController:
    def __init__(
        self,
        gates: Sequence[GateInterface] | None = None,
        policy: CascadePolicy | None = None,
        allrad_engine: ALLRADEngine | None = None,
        capital_engine: CapitalEngine | None = None,
    ):
        self._gates = list(gates) if gates is not None else build_default_gate_stack()
        self._policy = policy or CascadePolicy()
        self._allrad_engine = allrad_engine
        self._capital_engine = capital_engine

    @property
    def policy(self) -> CascadePolicy:
        return self._policy

    def run(
        self,
        universe: Sequence[str],
        context: CascadeContext | None = None,
        portfolio_state: Mapping[str, Any] | None = None,
        market_state: Mapping[str, Any] | None = None,
    ) -> CascadeContext:
        ctx = context or CascadeContext()
        current_symbols = list(universe)
        portfolio_state = portfolio_state or {}
        market_state = market_state or {}

        for gate in self._policy.order_gates(self._gates):
            if not current_symbols:
                ctx.trace.append("cascade_complete:empty_universe")
                break

            gate_name = gate.name

            ctx.record_gate_start(gate_name, current_symbols)
            logger.info("cascade_gate_start", extra={"gate": gate_name, "count": len(current_symbols)})

            filtered = list(gate.filter_universe(current_symbols))
            if not filtered:
                ctx.record_gate_decision(gate_name, current_symbols, GateDecision.FAIL)
                ctx.record_rejected(gate_name, current_symbols)
                current_symbols = []
                continue

            gate.fit(filtered)
            if hasattr(gate, "set_context"):
                gate.set_context(ctx)
            outcome = gate.evaluate(filtered)
            artifacts = gate.emit_artifacts(filtered)

            ctx.record_gate_decision(gate_name, filtered, outcome.decision)
            if artifacts:
                ctx.record_gate_artifacts(gate_name, filtered, artifacts)

            eligible, rejected = route_symbols(filtered, outcome)
            ctx.record_rejected(gate_name, rejected)
            logger.info(
                "cascade_gate_end",
                extra={
                    "gate": gate_name,
                    "decision": outcome.decision.value,
                    "eligible": len(eligible),
                    "rejected": len(rejected),
                },
            )

            if outcome.decision != GateDecision.PASS:
                current_symbols = []
                continue

            if gate.name == "signal" and self._allrad_engine is not None:
                allrad_symbols: list[str] = []
                for symbol in eligible:
                    decision = self._allrad_engine.evaluate(
                        ctx,
                        portfolio_state,
                        {**market_state, "symbol": symbol},
                    )
                    ctx.artifacts.setdefault("allrad", {})[symbol] = {
                        "decision": decision.allow_trade,
                        "max_exposure": decision.max_exposure,
                        "execution_override": decision.execution_override,
                        "risk_flags": decision.risk_flags,
                        "reason": decision.reason,
                    }
                    if decision.allow_trade:
                        allrad_symbols.append(symbol)
                current_symbols = allrad_symbols
                if not current_symbols:
                    ctx.record_gate_decision("allrad", eligible, GateDecision.FAIL)
                    break
                continue

            if gate.name == "execution" and self._capital_engine is not None:
                capital_symbols: list[str] = []
                capital_state = _resolve_capital_state(portfolio_state)
                for symbol in eligible:
                    execution_payload = _resolve_symbol_artifact(ctx.artifacts.get("execution", {}), symbol)
                    if not execution_payload:
                        continue
                    allrad_payload = _resolve_symbol_artifact(ctx.artifacts.get("allrad", {}), symbol)
                    allrad_decision = _risk_decision_from_payload(allrad_payload)
                    capital_decision = self._capital_engine.allocate(
                        execution_payload.get("execution_plan", {}),
                        allrad_decision,
                        capital_state,
                        {**market_state, "symbol": symbol},
                    )
                    ctx.artifacts.setdefault("capital", {})[symbol] = {
                        "allow_trade": capital_decision.allow_trade,
                        "position_size": capital_decision.position_size,
                        "capital_used": capital_decision.capital_used,
                        "exposure_after": capital_decision.exposure_after,
                        "sizing_reason": capital_decision.sizing_reason,
                        "risk_flags": capital_decision.risk_flags,
                        "symbol": capital_decision.symbol or symbol,
                        "execution_plan": capital_decision.execution_plan,
                    }
                    if capital_decision.allow_trade:
                        capital_symbols.append(symbol)
                current_symbols = capital_symbols
                if not current_symbols:
                    ctx.record_gate_decision("capital", eligible, GateDecision.FAIL)
                    break
                continue

            current_symbols = eligible

        return ctx


def _resolve_capital_state(portfolio_state: Mapping[str, Any]) -> CapitalState:
    state = portfolio_state.get("capital_state")
    if isinstance(state, CapitalState):
        return state
    total_equity = float(portfolio_state.get("total_equity", 0.0))
    return CapitalState(
        total_equity=total_equity,
        free_equity=float(portfolio_state.get("free_equity", total_equity)),
        used_margin=float(portfolio_state.get("used_margin", 0.0)),
        open_positions=int(portfolio_state.get("open_positions", 0)),
        net_exposure=float(portfolio_state.get("net_exposure", 0.0)),
        gross_exposure=float(portfolio_state.get("gross_exposure", 0.0)),
        realized_pnl=float(portfolio_state.get("realized_pnl", 0.0)),
        unrealized_pnl=float(portfolio_state.get("unrealized_pnl", 0.0)),
    )


def _resolve_symbol_artifact(artifacts: Mapping[str, Any], symbol: str) -> Mapping[str, Any]:
    payload = artifacts.get(symbol, {})
    if isinstance(payload, Mapping) and symbol in payload:
        nested = payload.get(symbol, {})
        return nested if isinstance(nested, Mapping) else {}
    return payload if isinstance(payload, Mapping) else {}


def _risk_decision_from_payload(payload: Mapping[str, Any]) -> RiskDecision:
    if not payload:
        return RiskDecision(
            allow_trade=True,
            max_exposure=1.0,
            execution_override=None,
            risk_flags={},
            reason="OK",
        )
    return RiskDecision(
        allow_trade=bool(payload.get("decision", True)),
        max_exposure=float(payload.get("max_exposure", 1.0)),
        execution_override=payload.get("execution_override"),
        risk_flags=dict(payload.get("risk_flags", {})),
        reason=str(payload.get("reason", "OK")),
    )
