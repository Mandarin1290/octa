# OCTA Context Freeze (Source of Truth)
Last updated: 2026-01-23 (Batch 28)

## Purpose
This file is the single source of truth when chat/tool context is low. It captures the verified repo state, boundaries, and non-negotiable rules.

## Verified State (Batch 28)
- Eliminated runtime imports from octa/core → legacy trees.
- Moved altdata and stream support modules into core and added legacy-path shims for backward compatibility.
- Updated core modules to import from core paths only.
- Added boundary guard test: tests/test_core_boundaries.py.
- Updated config/mypy.ini (incl. pydantic.* ignore) to keep typed-core lane green.
- Added core cascade skeleton in octa/core/cascade with controller, context, contracts, routing, policies, and tests.
- Added cascade adapters and registry for wiring gate stacks (SafeNoopGate fallback for missing gates).
- Added GlobalRegimeGate (1D) with regime labels (RISK_ON, RISK_OFF, REDUCE, HALT) and wired into registry.
- Added StructureGate (30M) with deterministic setup zone detection (breakout/pullback) and wired into registry.
- Added SignalGate (1H) with deterministic trade signals and wired into registry.
- Added ExecutionGate (5M) with deterministic execution plans (ENTER/HOLD, SL/TP) and wired into registry.
- Added MicroGate (1M) with micro optimization plan hints and wired into registry.
- Added shared OHLCVProvider contract and alignment helpers under octa/core/data/providers.
- Added ParquetOHLCVProvider for raw parquet ingestion and full system E2E test (1D/30M/1H/5M/1M coverage).
- Added paper-run cascade pipeline with JSONL audit logging under octa/core/pipeline.
- Canonical cascade timeframes: 1D, 30M, 1H, 5M, 1M.
- Added ALLRAD master risk engine under octa/core/risk/allrad with cascade hook after signal gate.
- Added CapitalEngine under octa/core/capital with sizing/exposure controls and cascade hook after execution gate.
- Added PortfolioEngine under octa/core/portfolio with global exposure and correlation controls.
- Added IBKR paper OMS under octa/core/execution with deterministic simulation mode and audit logging.
- Added analytics engine under octa/core/analytics with performance, risk, attribution, diagnostics.
- Added governance layer with audit chain, kill switch, and access policy under octa/core/governance.
- Added autonomy supervisor layer under octa/core/autonomy with health checks, runbooks, and safe-mode loop.
- Added autonomy hardening with execution_mode switching and audit-chain event logging.
- RiskDecision schema: allow_trade, max_exposure, execution_override, risk_flags, reason.
- Cascade hook location: after Signal gate (1H) before Execution gate (5M) in octa/core/cascade/controller.py.
- CapitalDecision schema: allow_trade, position_size, capital_used, exposure_after, sizing_reason, risk_flags.
- Capital hook location: after Execution gate (5M) before Micro gate (1M) in octa/core/cascade/controller.py.
- PortfolioDecision schema: allow_trades, approved_trades, blocked_trades, exposure_after, correlation_penalty, portfolio_risk_flags, reason.
- Portfolio aggregation hook: octa/core/pipeline/paper_run.py (portfolio JSONL record).
- OMS execution record: order_id, symbol, side, qty, filled_qty, avg_fill_price, status, slippage, latency_ms, fills.
- Analytics summary: cumulative_return, cagr, max_drawdown, sharpe, sortino, mar; attribution per symbol/gate/regime/session.
- Audit chain: append-only JSONL with SHA256 chaining; tamper detection via verify().
- Kill switch triggers: execution failures, slippage, daily loss, system health.
- HealthReport schema: overall, subsystems, recommended_mode, reasons; RunbookPlan schema: actions, next_mode, halt.
- Fail-closed: CRITICAL health or audit failure triggers SAFE/HALT in supervisor.
- Execution modes: OMS (default) vs DECISIONS_ONLY (runbooks may disable OMS).
- Audit event logging prefers AuditChain; JSONL is fallback.
- Autonomy CLI entrypoint: `python -m octa.core.autonomy.cli`.

## Canonical Paths
- Core implementations live under: octa/core/**
- Altdata: octa/core/data/sources/altdata/**
- Stream:  octa/core/data/sources/stream/**
- Parquet provider: octa/core/data/providers/parquet.py
- System E2E test: octa/core/system_tests/test_full_autonomy_parquet_e2e.py
- Legacy paths MUST be shims that forward to core (no duplicated logic).

## Non-negotiable Rules
1) Do NOT delete existing functions.
2) Do NOT change semantics of existing functions.
3) Only refactor/extend via wrappers, adapters, shims, and new modules.
4) Repo must remain runnable after each step (compileall/pytest).
5) Core must NEVER import legacy at runtime (enforced by tests/test_core_boundaries.py).
6) Typed-core mypy lane must remain green:
   mypy --config-file config/mypy.ini octa/core

## Required Verification Commands
- python -m compileall -q .
- pytest -q tests/test_imports.py --maxfail=1 --disable-warnings
- pytest -q tests/test_core_boundaries.py --maxfail=1 --disable-warnings
- mypy --config-file config/mypy.ini octa/core

## Logs
- Restructure verification is tracked in: docs/RESTRUCTURE_VERIFICATION.md
