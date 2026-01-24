# OCTA Integration — QuantLib / OpenGamma / Security / Accounting / Control Plane

Stand: 2026-01-13

## 0) Workspace Scan (Read-only Findings)

**Entrypoints / Orchestrators (existing)**
- Training CLI: [octa_training/run_train.py](octa_training/run_train.py)
- Autopilot (universe→gates→cascade→promotion→paper): [scripts/octa_autopilot.py](scripts/octa_autopilot.py)
- Sample pipeline daemon: [scripts/auto_pipeline_daemon.py](scripts/auto_pipeline_daemon.py)

**Training pipeline + gating (existing)**
- Main pipeline runner and gate evaluation: [octa_training/core/pipeline.py](octa_training/core/pipeline.py)
- Config loader merges global HF defaults: [octa_training/core/config.py](octa_training/core/config.py)
- Global gate thresholds live in: [configs/hf_defaults.yaml](configs/hf_defaults.yaml)

**Execution / IBKR (existing)**
- Paper execution path: [octa_ops/autopilot/paper_runner.py](octa_ops/autopilot/paper_runner.py)
- IBKR sandbox/contract adapter: [octa_vertex/broker/ibkr_contract.py](octa_vertex/broker/ibkr_contract.py)
- Optional real IBKR via ib_insync (already present): [octa_vertex/broker/ibkr_ib_insync.py](octa_vertex/broker/ibkr_ib_insync.py)

**Audit / Logs (existing)**
- Ledger-like audit mechanisms already exist (e.g. `octa_ledger`, NDJSON diagnostics in scripts).

## 1) Integration Map (Where to plug in new subsystems)

All integration points are adapters + feature flags. Existing behavior is unchanged unless you launch via the new wrapper entrypoint.

- **Startup dependency verifier/installer**
  - New wrapper entrypoint: [scripts/run_octa.py](scripts/run_octa.py)
  - Installs only what is enabled in feature flags; aborts start on critical install failures.

- **Gate tuning overlay (non-destructive)**
  - Implemented as an *optional overlay layer* inside the existing HF-default merge.
  - Triggered only by env var `OCTA_GATE_OVERLAY_PATH` (set by wrapper when `features.gate_tuning.enabled=true`).
  - Hook: [octa_training/core/config.py](octa_training/core/config.py)

- **QuantLib**
  - Adapter module: [octa_core/risk_institutional/quantlib_adapter.py](octa_core/risk_institutional/quantlib_adapter.py)
  - Intended use: option pricing + curve shocks for stress/risk enrichment.
  - Enabled only when `features.quantlib.enabled=true`.

- **OpenGamma (service integration, not pip)**
  - REST client: [octa_core/risk_institutional/opengamma_client.py](octa_core/risk_institutional/opengamma_client.py)
  - Aggregator hook: [octa_core/risk_institutional/risk_aggregator.py](octa_core/risk_institutional/risk_aggregator.py)
  - Local dev service harness: [docker/docker-compose.opengamma.yml](docker/docker-compose.opengamma.yml)
  - Fail-closed rule: if `features.opengamma.required_for_live=true` and service unhealthy → live start is blocked.

- **Security stack**
  - Secrets abstraction: [octa_core/security/secrets.py](octa_core/security/secrets.py)
  - Encryption primitives: [octa_core/security/crypto.py](octa_core/security/crypto.py)
  - Encryption-at-rest policy helper: [octa_core/security/at_rest.py](octa_core/security/at_rest.py)
  - Append-only hash-chain audit log: [octa_core/security/audit.py](octa_core/security/audit.py)

- **Accounting/Controlling**
  - Double-entry SQLite ledger: [octa_core/accounting/ledger.py](octa_core/accounting/ledger.py)
  - NAV/PnL snapshot: [octa_core/accounting/valuations.py](octa_core/accounting/valuations.py)
  - DATEV export (minimal): [octa_core/accounting/exports_datev.py](octa_core/accounting/exports_datev.py)
  - SafeDex connector interface + queued HTTP implementation: [octa_core/accounting/safedex_connector.py](octa_core/accounting/safedex_connector.py)
  - Enabled only when `features.accounting.enabled=true`.

- **Control plane (Web + Telegram)**
  - FastAPI app: [octa_core/control_plane/api.py](octa_core/control_plane/api.py)
  - Safe stop implementation: [octa_core/control_plane/safety_stop.py](octa_core/control_plane/safety_stop.py)
  - Telegram bot adapter: [octa_core/control_plane/telegram_bot.py](octa_core/control_plane/telegram_bot.py)
  - Start via: [scripts/run_dashboard.py](scripts/run_dashboard.py)

## 2) How to start (No changes to existing entrypoints)

**Wrapper entrypoint (recommended):**
- Paper/autopilot: `python scripts/run_octa.py --start paper --mode autopilot --autopilot-config configs/autonomous_paper.yaml`
- Control plane: `python scripts/run_dashboard.py --features octa_core/config/octa_features.yaml`

**OpenGamma local harness:**
- `bash scripts/run_opengamma.sh` (uses `docker/docker-compose.opengamma.yml`)

No existing scripts are modified to import the wrapper; you opt in by using the new entrypoints.

## 3) Gate tuning usage (safe)

- Enable in [octa_core/config/octa_features.yaml](octa_core/config/octa_features.yaml):
  - `features.gate_tuning.enabled: true`
- Overlay config: [octa_core/config/risk_overlays.yaml](octa_core/config/risk_overlays.yaml)
- The wrapper exports `OCTA_GATE_OVERLAY_PATH=...` which enables the overlay merge.

## 4) File-by-file implementation plan (what was added)

- Wrapper bootstrap:
  - [octa_core/bootstrap/deps.py](octa_core/bootstrap/deps.py)
  - [octa_core/bootstrap/env_check.py](octa_core/bootstrap/env_check.py)
  - [octa_core/bootstrap/start_guard.py](octa_core/bootstrap/start_guard.py)
  - [scripts/run_octa.py](scripts/run_octa.py)

- Security:
  - [octa_core/security/audit.py](octa_core/security/audit.py)
  - [octa_core/security/secrets.py](octa_core/security/secrets.py)
  - [octa_core/security/crypto.py](octa_core/security/crypto.py)
  - [octa_core/security/at_rest.py](octa_core/security/at_rest.py)

- Risk institutional:
  - [octa_core/risk_institutional/quantlib_adapter.py](octa_core/risk_institutional/quantlib_adapter.py)
  - [octa_core/risk_institutional/opengamma_client.py](octa_core/risk_institutional/opengamma_client.py)
  - [octa_core/risk_institutional/risk_aggregator.py](octa_core/risk_institutional/risk_aggregator.py)
  - [octa_core/risk_institutional/stress_scenarios.py](octa_core/risk_institutional/stress_scenarios.py)

- Accounting:
  - [octa_core/accounting/ledger.py](octa_core/accounting/ledger.py)
  - [octa_core/accounting/valuations.py](octa_core/accounting/valuations.py)
  - [octa_core/accounting/ifrs_hgb_mapper.py](octa_core/accounting/ifrs_hgb_mapper.py)
  - [octa_core/accounting/exports_datev.py](octa_core/accounting/exports_datev.py)
  - [octa_core/accounting/safedex_connector.py](octa_core/accounting/safedex_connector.py)

- Control plane:
  - [octa_core/control_plane/api.py](octa_core/control_plane/api.py)
  - [octa_core/control_plane/safety_stop.py](octa_core/control_plane/safety_stop.py)
  - [octa_core/control_plane/telegram_bot.py](octa_core/control_plane/telegram_bot.py)
  - [octa_core/control_plane/dashboard.py](octa_core/control_plane/dashboard.py)
  - [scripts/run_dashboard.py](scripts/run_dashboard.py)

- Config:
  - [octa_core/config/octa_features.yaml](octa_core/config/octa_features.yaml)
  - [octa_core/config/risk_overlays.yaml](octa_core/config/risk_overlays.yaml)
  - [octa_core/config/security.yaml](octa_core/config/security.yaml)
  - [octa_core/config/opengamma.yaml](octa_core/config/opengamma.yaml)
  - [octa_core/config/accounting.yaml](octa_core/config/accounting.yaml)

- OpenGamma docker harness:
  - [docker/docker-compose.opengamma.yml](docker/docker-compose.opengamma.yml)

- Minimal smoke tests:
  - [tests/test_bootstrap_deps_selection.py](tests/test_bootstrap_deps_selection.py)
  - [tests/test_security_audit_hash_chain.py](tests/test_security_audit_hash_chain.py)
  - [tests/test_control_plane_safe_stop.py](tests/test_control_plane_safe_stop.py)
  - [tests/test_opengamma_healthcheck_optional.py](tests/test_opengamma_healthcheck_optional.py)

## 5) Checklist (touched vs added)

**Touched (existing files)**
- [octa_training/core/config.py](octa_training/core/config.py) — optional overlay merge via `OCTA_GATE_OVERLAY_PATH`
- [octa_ops/autopilot/paper_eval.py](octa_ops/autopilot/paper_eval.py) — per symbol/timeframe eval matrix (fail-closed HOLD default)

**Added (new files)**
- All files listed in section 4 above, plus:
  - [scripts/run_octa.py](scripts/run_octa.py)
  - [scripts/run_dashboard.py](scripts/run_dashboard.py)
  - [octa_core/risk_institutional/opengamma_stub_service.py](octa_core/risk_institutional/opengamma_stub_service.py)

