# OCTA Refactor Audit Report

## Executive Summary
- Scope: architecture and redundancy audit only; no behavior changes, no deletions, no renames.
- The workspace is large, with heavy concentration in `tests/`, `scripts/`, and `octa_training/` + `octa_core/`.
- High fan-in modules indicate central orchestration and config/state coupling; high fan-out modules are entrypoints and pipelines.
- KPI math is implemented in multiple places with differing annualization assumptions and drawdown definitions; this is the largest compliance risk for HF-grade KPIs.

## Assumptions (Explicit)
- The repo root is `/home/n-b/Octa` and all analysis is local, no network.
- Files under `raw/`, `reports/`, `artifacts/`, and `mlruns/` are data artifacts, not source.
- Name-based responsibility inference is acceptable (package naming conventions appear consistent).
- Import graph uses static AST parsing and first-party module prefixes only; dynamic imports may be missed.

## Architecture Map

### Top-Level Directories and Responsibilities (inferred)
- `octa_training/`: core training pipelines, evaluation, gates, packaging, tuning.
- `octa_core/`: core system utilities (risk, control plane, accounting, risk institutional adapters).
- `octa_strategy/`: strategy lifecycle, gates, health, stability, drawdown analysis.
- `octa_vertex/`: execution, brokerage adapters, paper/shadow execution, slippage, kill enforcement.
- `octa_sentinel/`: safety policies, drawdown playbook, kill switches, monitoring gates.
- `octa_ops/`: orchestration, autopilot, runbooks, incident handling.
- `octa_nexus/`: orchestration/runtime bus and supervisor.
- `octa_ledger/`: performance, attribution, incident records, storage.
- `octa_fabric/`: config/loader/fingerprint and environment settings.
- `octa_reports/`: report generation and dashboards.
- `octa_assets/`: domain asset models (fx, rates, futures, commodities, vol).
- `octa_alpha/`: alpha pipeline, signals, regime scoring, pre-risk checks.
- `okta_altdat/` + `octa_altdata/`: alternative data features, connectors, storage.
- `scripts/`: executable entrypoints, data prep, diagnostics, batch flows.
- `tools/`: utility scripts for gating and training operations.
- `tests/` + `octa_tests/`: test suites, drills, scenarios.
- `dagster/`: pipeline definitions for orchestration.

### Module Density (file counts)
- Very high: `raw/` (7211 files), `reports/` (5376), `tests/` (875; 220 py).
- High code density: `scripts/` (212; 102 py), `octa_core/` (116; 55 py), `octa_training/` (61; 30 py), `octa_ops/` (58; 29 py).
- Mid: `octa_strategy/` (44; 22 py), `octa_reports/` (41; 20 py), `octa_alpha/` (38; 19 py), `octa_assets/` (38; 19 py), `octa_vertex/` (38; 19 py).

### Entry Points (non-exhaustive)
- Core training: `octa_training/run_train.py`, `octa_training/run_daemon.py`.
- Cascade training: `scripts/train_multiframe_symbol.py`, `scripts/e2e_orchestrator.py`.
- Autopilot: `scripts/octa_autopilot.py`, `octa_ops/autopilot/*`.
- Paper/live orchestration: `scripts/run_nexus_paper.py`, `octa_nexus/orchestrator.py`.
- Gate/diagnostics: `scripts/global_gate_diagnose.py`, `scripts/batch_gate_check_indices_1d.py`, `scripts/batch_gate_check_indices_multitf.py`.
- Data tooling: `scripts/convert_*`, `scripts/normalize_parquets.py`, `scripts/prepare_options_labels.py`.
- Dagster: `dagster/repository.py`, `dagster/jobs.py`.

## Dependency Graph Analysis

### Static Import Graph Summary
- Modules analyzed (first-party only): 721
- Import edges: 781
- Cycles detected (static):
  - `scripts.hyperparam_search` -> `scripts.train_and_save` -> `scripts.hyperparam_search`

### High Fan-Out Nodes (potential god/entry modules)
- `octa_training/core/pipeline.py` (fan-out 18)
- `octa_training/run_train.py` (fan-out 18)
- `scripts/train_and_save.py` (fan-out 15)
- `scripts/train_multiframe_symbol.py` (fan-out 13)
- `octa_nexus/paper_runtime.py` (fan-out 10)

### High Fan-In Nodes (central services)
- `octa_training/core/config.py` (fan-in 25)
- `octa_training/core/state.py` (fan-in 20)
- `octa_training/core/io_parquet.py` (fan-in 18)
- `octa_ledger/store.py` (fan-in 18)
- `octa_training/core/pipeline.py` (fan-in 17)
- `octa_ledger/core.py` (fan-in 15)

### Coupling Observations
- Training stack is tightly coupled to `octa_training/core/config.py`, `octa_training/core/state.py`, and `octa_training/core/io_parquet.py`.
- Scripts provide many parallel entrypoints that re-implement config and logging flows, increasing inconsistency risk.

## Redundancy Clusters

### SAFE to unify (infra-level only)
- Config loading: repeated `yaml.safe_load` helpers and ad-hoc overlays.
  - Examples: `octa_training/core/config.py`, `scripts/octa_autopilot.py`, `scripts/run_dashboard.py`, `scripts/run_octa.py`, `octa_core/control_plane/api.py`, `octa_ops/autopilot/universe.py`, `octa_stream/manifest.py`.
- Logging setup: mixed `logging.basicConfig` in multiple scripts vs structured logger in `octa_training/core/logging.py`.
  - Examples: `scripts/train_options_time_series.py`, `scripts/train_options_lstm.py`, `scripts/run_all_assets.py`, `octa_training/core/logging.py`.
- Path/env utilities: multiple local patterns for resolving config and root paths.
  - Examples: `octa_training/core/fs_utils.py`, `scripts/train_and_save.py`, `octa_fabric/loader.py`.

### MEDIUM risk (requires careful validation)
- Data loading and parquet handling: parallel loaders and converters.
  - Examples: `octa_training/core/io_parquet.py`, `scripts/normalize_parquets.py`, `scripts/convert_*`, `octa_stream/manifest.py`.
- Feature engineering pipelines: overlapping feature builders for altdata vs training.
  - Examples: `octa_training/core/features.py`, `okta_altdat/features/feature_builder.py`.
- Dataset splitting: multiple CV/walk-forward implementations.
  - Examples: `octa_training/core/splits.py`, `scripts/cross_validation.py`, `scripts/train_and_save.py`.

### HIGH risk (do not unify without explicit sign-off)
- Training, scoring, gating: overlapping performance metrics and gate checks.
  - Examples: `octa_training/core/evaluation.py`, `octa_training/core/gates.py`, `octa_strategy/paper_gates.py`, `octa_sentinel/paper_gates.py`, `scripts/batch_train_and_gate.py`.
- Execution and portfolio logic: `octa_vertex/`, `octa_strategy/`, `octa_sentinel/`.
- KPI computation: `octa_training/core/evaluation.py`, `octa_ledger/performance.py`, `octa_reports/investor_reports.py`, `scripts/backtest.py`, `octa_alpha/pre_risk.py`.

## KPI Inconsistency Audit (HF KPI risk)

### Sharpe / Sortino
- `octa_training/core/evaluation.py` uses inferred annualization based on bar frequency, log returns, and costs; Sharpe = mean_ann / vol_ann.
- `octa_ledger/performance.py` uses mean return and `annualize_return` with (1+mu)^periods_per_year and `annualized_vol`; includes risk_free.
- `scripts/backtest.py` uses mean/std * sqrt(252) with sample std ddof=1 and synthetic returns.
- Risk: reported Sharpe may differ across training vs ledger vs backtest vs diagnostics, undermining KPI consistency.

### Max Drawdown
- `octa_training/core/evaluation.py` computes max drawdown on equity from log-return series; exposes positive fraction.
- `octa_ledger/performance.py` computes max drawdown from price series; returns (max_dd, duration).
- `octa_reports/investor_reports.py` computes max drawdown from NAV series.
- `octa_alpha/pre_risk.py` computes max drawdown from compounded simple returns.
- Risk: inconsistent series inputs (prices vs equity vs nav vs returns) and definitions may alter drawdown by regime and timeframe.

### Annualization Factors
- `octa_training/core/evaluation.py` infers annualization factor from timestamp deltas (daily/hourly/minute-ish).
- `octa_ledger/performance.py` uses explicit `periods_per_year` default 252.
- `octa_reports/investor_reports.py` uses 365 days for return annualization and 252 for volatility.
- `scripts/backtest.py` uses sqrt(252) for Sharpe and builds equity as cumulative returns.
- Risk: KPI drift across reports, training gates, and ledger metrics; potential compliance inconsistency.

## Safe Refactor Strategy (Additive Only)

### Proposed Additive Layer
- Introduce `octa_core/shared/` or `octa_training/shared/` utilities for:
  - YAML config loading with overlays
  - Logging setup with structured JSON + console
  - Path/env helpers (repo root, data paths)
- Existing modules keep signatures and behavior; only delegate internal helper calls.

### Proposed Changes (No behavior changes)
1) Shared YAML loader
   - Risk: Low
   - Complexity: S
   - Benefit: Consistent config reading, fewer ad-hoc loaders
   - Rollback: Remove delegation lines, keep local helpers
   - Validation: Compare loaded config dicts byte-for-byte across tools

2) Shared logging setup
   - Risk: Low
   - Complexity: S
   - Benefit: uniform logs, easier audit trails
   - Rollback: revert to local logging.basicConfig
   - Validation: verify log formatting and level parity

3) Shared path/env resolver
   - Risk: Low
   - Complexity: S
   - Benefit: consistent file locations, fewer path bugs
   - Rollback: return to inline path code
   - Validation: compare resolved paths against prior outputs

## Do-Not-Touch Registry (Behavior-Critical)
- Trading/execution: `octa_vertex/`, `octa_strategy/`, `octa_sentinel/`.
- Training/gating core logic: `octa_training/core/` (except infra-only helpers).
- Risk/capital management: `octa_core/`, `octa_capital/`, `octa_fund/`.
- Governance/audit/compliance: `octa_governance/`, `octa_audit/`, `octa_reg/`, `octa_compliance/`.

## Risk Register
- KPI drift due to inconsistent Sharpe/Sortino/MaxDD definitions across subsystems.
- Multiple config loaders with inconsistent overlays and default handling.
- Mixed logging setups (structured vs basicConfig) reduce auditability.
- Ad-hoc data prep scripts in `scripts/` bypass core loaders and validation.
- Hidden dependency cycles in scripts (hyperparam search) can complicate reuse.

## Validation Plan (Required for Any Refactor)
- Golden-run reproducibility: same inputs -> same outputs across training/gating.
- Output artifact hashing: compare packaged artifacts and metadata hashes.
- Logging parity: structured log lines count and key fields match.
- Performance baseline: training runtime within tolerance; memory footprint stable.
- Risk-gate parity: PASS/FAIL decisions identical on fixed datasets.

