from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from octa.core.cascade.context import CascadeContext
from octa.core.cascade.contracts import GateDecision
from octa.core.cascade.controller import CascadeController
from octa.core.cascade.registry import build_default_gate_stack
from octa.core.data.providers.in_memory import InMemoryOHLCVProvider
from octa.core.data.providers.ohlcv import OHLCVBar, OHLCVProvider, Timeframe
from octa.core.portfolio.engine import PortfolioEngine, PortfolioEngineConfig
from octa.core.portfolio.state import PortfolioState
from octa.core.analytics.performance import compute_performance
from octa.core.governance.audit_chain import AuditChain

_TIMEFRAMES: tuple[Timeframe, ...] = ("1D", "30M", "1H", "5M", "1M")


def run_paper_cascade(
    symbols: Sequence[str],
    provider: OHLCVProvider,
    start: datetime | None,
    end: datetime | None,
    audit_path: Path,
    *,
    deterministic_seed: int = 42,
    portfolio_engine: PortfolioEngine | None = None,
    execution_mode: str = "OMS",
    audit_chain: AuditChain | None = None,
) -> None:
    del deterministic_seed
    gates = build_default_gate_stack(ohlcv_provider=provider)
    controller = CascadeController(gates=gates)
    ordered_gates = controller.policy.order_gates(gates)

    audit_path.parent.mkdir(parents=True, exist_ok=True)

    with audit_path.open("w", encoding="utf-8") as handle:
        symbol_records: list[dict[str, Any]] = []
        capital_decisions: list[Mapping[str, Any]] = []
        for symbol in symbols:
            _prefetch_all(provider, symbol, start, end)
            context = CascadeContext()
            context = controller.run([symbol], context)

            for gate in ordered_gates:
                decision = _resolve_decision(context, gate.name, symbol)
                artifacts = _resolve_artifacts(context, gate.name, symbol)
                metrics, quality_flags = _split_artifacts(artifacts)
                record = {
                    "ts": _resolve_timestamp(provider, symbol, gate.timeframe, end),
                    "symbol": symbol,
                    "gate": gate.name,
                    "timeframe": gate.timeframe,
                    "decision": decision,
                    "artifacts": artifacts,
                    "metrics": metrics,
                    "quality_flags": quality_flags,
                    "execution_mode": execution_mode,
                }
                symbol_records.append(record)

            capital_payload = _resolve_artifacts(context, "capital", symbol)
            if capital_payload:
                capital_decisions.append({"symbol": symbol, **capital_payload})

        if portfolio_engine is None:
            portfolio_engine = PortfolioEngine(PortfolioEngineConfig())

        portfolio_state = PortfolioState()
        if portfolio_engine is not None:
            portfolio_decision = portfolio_engine.aggregate(
                _capital_decisions_from_payloads(capital_decisions),
                portfolio_state,
                {"total_equity": 100_000.0},
            )
            record_ts = _resolve_portfolio_ts(symbols, provider, end)
            portfolio_record = _portfolio_record(portfolio_decision, record_ts, execution_mode)
            handle.write(json.dumps(portfolio_record, sort_keys=True))
            handle.write("\n")
            if audit_chain is not None:
                audit_chain.append({"event": "portfolio", **portfolio_record})

        if execution_mode == "DECISIONS_ONLY":
            exec_record = {
                "ts": (end or datetime(1970, 1, 1)).isoformat(),
                "symbol": "PORTFOLIO",
                "gate": "oms",
                "timeframe": "EXEC",
                "decision": "EXEC_DISABLED",
                "artifacts": {},
                "metrics": {},
                "quality_flags": {},
                "execution_mode": execution_mode,
            }
            handle.write(json.dumps(exec_record, sort_keys=True))
            handle.write("\n")
            if audit_chain is not None:
                audit_chain.append({"event": "execution", **exec_record})

        for record in symbol_records:
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")

        if portfolio_engine is not None:
            equity_curve = [100_000.0 for _ in range(len(symbols) + 1)]
            performance = compute_performance(equity_curve)
            summary_path = audit_path.parent / "performance_summary.json"
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_path.write_text(
                json.dumps(
                    {
                        "cumulative_return": performance.cumulative_return,
                        "cagr": performance.cagr,
                        "max_drawdown": performance.max_drawdown,
                        "sharpe": performance.sharpe,
                        "sortino": performance.sortino,
                        "mar": performance.mar,
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )


def _resolve_timestamp(
    provider: OHLCVProvider, symbol: str, timeframe: Timeframe, end: datetime | None
) -> str:
    bars = provider.get_ohlcv(symbol, timeframe, end=end)
    if end is not None:
        return end.isoformat()
    if bars:
        return bars[-1].ts.isoformat()
    return datetime(1970, 1, 1).isoformat()


def _prefetch_all(
    provider: OHLCVProvider, symbol: str, start: datetime | None, end: datetime | None
) -> None:
    for timeframe in _TIMEFRAMES:
        provider.get_ohlcv(symbol, timeframe, start=start, end=end)


def _resolve_decision(context: CascadeContext, gate: str, symbol: str) -> str:
    decisions = context.decisions.get(gate, {})
    decision = decisions.get(symbol, GateDecision.FAIL)
    if isinstance(decision, GateDecision):
        return decision.value
    return str(decision)


def _resolve_artifacts(context: CascadeContext, gate: str, symbol: str) -> dict[str, Any]:
    artifacts = context.artifacts.get(gate, {})
    if isinstance(artifacts, dict) and symbol in artifacts:
        payload = artifacts[symbol]
        if isinstance(payload, dict) and symbol in payload:
            return payload[symbol]
        if isinstance(payload, dict):
            return payload
    return {}


def _split_artifacts(artifacts: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    metrics = {}
    quality_flags = {}
    if "metrics" in artifacts:
        metrics = artifacts.get("metrics", {})
    if "quality_flags" in artifacts:
        quality_flags = artifacts.get("quality_flags", {})
    if "structure_metrics" in artifacts:
        metrics = artifacts.get("structure_metrics", {})
    if "signal_metrics" in artifacts:
        metrics = artifacts.get("signal_metrics", {})
    if "execution_metrics" in artifacts:
        metrics = artifacts.get("execution_metrics", {})
    if "micro_metrics" in artifacts:
        metrics = artifacts.get("micro_metrics", {})
    if "quality_flags" in artifacts:
        quality_flags = artifacts.get("quality_flags", {})
    if "micro_risk_flags" in artifacts:
        quality_flags = artifacts.get("micro_risk_flags", {})
    return dict(metrics), dict(quality_flags)


def _capital_decisions_from_payloads(payloads: Sequence[Mapping[str, Any]]):
    from octa.core.capital.engine import CapitalDecision

    decisions: list[CapitalDecision] = []
    for payload in payloads:
        decisions.append(
            CapitalDecision(
                allow_trade=bool(payload.get("allow_trade", False)),
                position_size=float(payload.get("position_size", 0.0)),
                capital_used=float(payload.get("capital_used", 0.0)),
                exposure_after=float(payload.get("exposure_after", 0.0)),
                sizing_reason=str(payload.get("sizing_reason", "")),
                risk_flags=dict(payload.get("risk_flags", {})),
                symbol=payload.get("symbol"),
                execution_plan=payload.get("execution_plan"),
            )
        )
    return decisions


def _portfolio_record(decision: Any, record_ts: datetime, execution_mode: str) -> dict[str, Any]:
    return {
        "ts": record_ts.isoformat(),
        "symbol": "PORTFOLIO",
        "gate": "portfolio",
        "timeframe": "PORTFOLIO",
        "decision": "PASS" if decision.allow_trades else "FAIL",
        "artifacts": {
            "approved_trades": len(decision.approved_trades),
            "blocked_trades": len(decision.blocked_trades),
            "exposure_after": decision.exposure_after,
            "correlation_penalty": decision.correlation_penalty,
            "portfolio_risk_flags": decision.portfolio_risk_flags,
            "reason": decision.reason,
        },
        "metrics": {},
        "quality_flags": {},
        "execution_mode": execution_mode,
    }


def _resolve_portfolio_ts(
    symbols: Sequence[str], provider: OHLCVProvider, end: datetime | None
) -> datetime:
    if end is not None:
        return end
    latest: datetime | None = None
    for symbol in symbols:
        bars = provider.get_ohlcv(symbol, "1D")
        if not bars:
            continue
        candidate = bars[-1].ts
        if latest is None:
            latest = candidate
            continue
        if _coerce_datetime(candidate) > _coerce_datetime(latest):
            latest = candidate
    return latest or datetime(1970, 1, 1)


def _coerce_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _build_synthetic_bars(
    start: datetime, count: int, step: timedelta, start_price: float, step_price: float
) -> list[OHLCVBar]:
    bars: list[OHLCVBar] = []
    price = start_price
    for idx in range(count):
        ts = start + step * idx
        close = price
        high = close * 1.01
        low = close * 0.99
        bars.append(OHLCVBar(ts=ts, open=close, high=high, low=low, close=close, volume=10_000))
        price += step_price
    return bars


def _seeded_series(symbol: str, base: float, increment: float) -> tuple[float, float]:
    total = sum(ord(char) for char in symbol)
    return base + total * 0.01, increment + (total % 5) * 0.0001


def _populate_provider(
    provider: InMemoryOHLCVProvider, symbols: Iterable[str], bars: int, start: datetime
) -> None:
    steps = {
        "1D": timedelta(days=1),
        "30M": timedelta(minutes=30),
        "1H": timedelta(hours=1),
        "5M": timedelta(minutes=5),
        "1M": timedelta(minutes=1),
    }
    for symbol in symbols:
        base, increment = _seeded_series(symbol, 100.0, 0.02)
        for timeframe, step in steps.items():
            bars_list = _build_synthetic_bars(start, bars, step, base, increment)
            provider.set_bars(symbol, timeframe, bars_list)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run cascade paper pipeline with synthetic data.")
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols")
    parser.add_argument("--bars", type=int, default=500, help="Bars per timeframe")
    parser.add_argument("--output", type=Path, default=Path("reports/paper_run.jsonl"))
    args = parser.parse_args(argv)

    symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()]
    provider = InMemoryOHLCVProvider()
    start = datetime(2024, 1, 1)
    _populate_provider(provider, symbols, args.bars, start)

    portfolio_engine = PortfolioEngine(PortfolioEngineConfig())
    run_paper_cascade(symbols, provider, start, None, args.output, portfolio_engine=portfolio_engine)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
