# RESTRUCTURE_PLAN (Phase 0 – Discovery, Read Only)

## 1) Current High-Level Tree (Top 2 Levels)
```
=1.2.0
AGENTS.md
Dockerfile
MLFLOW_README.md
OCTA_AUDIT_REPORT.md
OCTA_FIX_PACK/
  0001_paper_deploy_fix.patch
  0002_broker_adapter.patch
  MIGRATION_NOTES_0001_PAPER_DEPLOY.md
  README.md
OCTA_LIBRARY_MAP.md
README.md
README_TRAINING.md
README_integration.md
REPORT_REFACTOR_AUDIT.json
REPORT_REFACTOR_AUDIT.md
__pycache__/
  download_aapl_1m.cpython-313.pyc
  test_pipeline.cpython-313.pyc
artifacts/
  canary/
  dvc_remote/
  feature_specs/
  models/
assets/
  asset_map.yaml
catboost_info/
  catboost_training.json
  learn/
  learn_error.tsv
  test/
  test_error.tsv
  time_left.tsv
  tmp/
config/
  altdat.yaml
  symbol_universe/
configs/
  _auto/
  asset/
  asset_profiles_example.yaml
  autonomous_paper.yaml
  base.yaml
  cascade_hf.yaml
  dev.yaml
  e2e_real_raw.yaml
  e2e_real_raw_debug.yaml
  gate_overlay_relax.yaml
  gate_overlay_relax_quality.yaml
  hf_defaults.yaml
  live.yaml
  paper.yaml
  tmp_djr.yaml
  tmp_smoke_djr.yaml
  tmp_train_djr.yaml
  tmp_train_djr_multitf.yaml
  tmp_train_djr_multitf_hf.yaml
  tmp_train_djr_multitf_hf_30m_hedgefund.yaml
  validation_thresholds.json
core/
  __pycache__/
  training_safety_lock.py
coverage.ini
dagster/
  README.md
  __init__.py
  __pycache__/
  jobs.py
  ops/
  repository.py
deploy/
  case_c/
docker/
  docker-compose.opengamma.yml
  monitoring/
docker-compose.redis.yml
docs/
  AGENT_STATE.md
  (many markdowns...)
download_aapl_1m.py
feast_repo/
  README.md
  __pycache__/
  data/
  feature_store.yaml
  features.py
  registry.db
fx_77_symbols.txt
fx_complete_symbols.txt
k8s/
  odm_trainer_job.yaml
  trainer_deployment.yaml
  trainer_hpa.yaml
logs/
  _cascade_hf/
  _e2e_real/
  octa_training.jsonl
  octa_training.log
  stock_convert_1m_fill.log
  stock_convert_30m_5m.log
  stock_convert_5m_fill.log
  stock_convert_resume.log
mlflow.db
mlruns/
  0/
models/
  AAPL_OPT_labeled_refined_lstm_torch.pt
  AAPL_OPT_labeled_refined_wf_model.pkl
  AAPL_OPT_labeled_wf_model.pkl
mypy.ini
octa_accounting/
  __init__.py
  __pycache__/
  cost_monitor.py
  fees.py
  nav_engine.py
octa_alpha/
  __init__.py
  __pycache__/
  (many .py files...)
... (full tree captured in discovery output)
```

## 2) Mapping Table (old_path → new_path → category)

Assumptions:
- Data/artifact directories (`raw/`, `reports/`, `mlruns/`, `models/`, `state/`, `tmp/`, `catboost_info/`) are treated as outputs and mapped under `artifacts/`.
- Legacy packages remain shimmable; new canonical code lives under `octa/`.
- This is a top-level mapping; submodule mapping within packages will be detailed before each micro-batch.

| old_path | new_path | category |
|---|---|---|
| AGENTS.md | docs/AGENTS.md | support (docs) |
| README.md | docs/README.md | support (docs) |
| README_TRAINING.md | docs/README_TRAINING.md | support (docs) |
| README_integration.md | docs/README_integration.md | support (docs) |
| MLFLOW_README.md | docs/MLFLOW_README.md | support (docs) |
| OCTA_AUDIT_REPORT.md | docs/OCTA_AUDIT_REPORT.md | support (docs) |
| OCTA_LIBRARY_MAP.md | docs/OCTA_LIBRARY_MAP.md | support (docs) |
| REPORT_REFACTOR_AUDIT.md | docs/REPORT_REFACTOR_AUDIT.md | support (docs) |
| REPORT_REFACTOR_AUDIT.json | docs/REPORT_REFACTOR_AUDIT.json | support (docs) |
| docs/ | docs/ | support (docs) |
| config/ | config/ | infra (config) |
| configs/ | config/ | infra (config) |
| scripts/ | scripts/ | infra (entrypoints) |
| tests/ | tests/ | infra (tests) |
| octa_tests/ | tests/octa_tests/ | infra (tests) |
| octa_training/ | octa/core/ (training pipeline; split into data/features/gates) | core |
| octa_core/ | octa/core/ (risk/portfolio/governance/data) | core |
| octa_strategy/ | octa/core/strategies/ | core |
| octa_strategies/ | octa/core/strategies/ | core |
| octa_vertex/ | octa/core/execution/ | core |
| octa_sentinel/ | octa/core/monitoring/ (safety + kill switch) | core |
| octa_alpha/ | octa/research/experiments/ or octa/core/features/ (to be resolved) | research/core |
| octa_assets/ | octa/core/data/sources/ | core |
| octa_stream/ | octa/core/data/sources/ | core |
| octa_ops/ | octa/infra/orchestration/ | infra |
| octa_nexus/ | octa/infra/orchestration/ | infra |
| dagster/ | octa/infra/orchestration/ | infra |
| octa_fabric/ | octa/infra/secrets/ + octa/infra/security/ | infra |
| octa_reports/ | octa/support/reporting/ | support |
| octa_accounting/ | octa/support/accounting/ | support |
| octa_ledger/ | octa/support/accounting/ | support |
| octa_fund/ | octa/support/accounting/ | support |
| octa_capital/ | octa/core/capital/ | core |
| octa_risk/ | octa/core/risk/ | core |
| octa_resilience/ | octa/infra/security/ or octa/core/monitoring/ | infra/core |
| octa_monitoring/ | octa/core/monitoring/ | core |
| octa_governance/ | octa/core/governance/ | core |
| octa_audit/ | octa/core/governance/ | core |
| octa_reg/ | octa/support/compliance/ | support |
| octa_compliance/ | octa/support/compliance/ | support |
| octa_security/ | octa/infra/security/ | infra |
| octa_ip/ | octa/core/governance/permissions/ | core |
| octa_legal/ | octa/support/compliance/ | support |
| octa_wargames/ | octa/research/experiments/ | research |
| octa_chaos/ | octa/research/experiments/ | research |
| okta_altdat/ | octa/core/data/sources/ | core |
| octa_altdata/ | octa/core/data/sources/ | core |
| feast_repo/ | octa/infra/orchestration/ | infra |
| tools/ | scripts/ (as legacy tooling) | infra |
| core/ | octa/core/governance/ (training safety lock) | core |
| src/ | octa/ (package migration placeholder) | core |
| assets/ | octa/core/data/sources/ | core |
| deploy/ | octa/infra/deployment/ | infra |
| docker/ | octa/infra/deployment/docker/ | infra |
| docker-compose.redis.yml | octa/infra/deployment/docker/docker-compose.redis.yml | infra |
| k8s/ | octa/infra/deployment/ | infra |
| artifacts/ | artifacts/ | infra (output) |
| logs/ | logs/ | infra (output) |
| raw/ | artifacts/raw/ | infra (output) |
| reports/ | artifacts/reports/ | infra (output) |
| mlruns/ | artifacts/mlruns/ | infra (output) |
| models/ | artifacts/models/ | infra (output) |
| state/ | artifacts/state/ | infra (output) |
| tmp/ | artifacts/tmp/ | infra (output) |
| catboost_info/ | artifacts/catboost_info/ | infra (output) |
| __pycache__/ | artifacts/__pycache__/ | infra (output) |
| download_aapl_1m.py | scripts/download_aapl_1m.py | infra (entrypoints) |
| test_pipeline.py | tests/test_pipeline.py | infra (tests) |
| sample.parquet | artifacts/sample.parquet | infra (output) |
| mlflow.db | artifacts/mlflow.db | infra (output) |
| fx_77_symbols.txt | artifacts/fx_77_symbols.txt | infra (output) |
| fx_complete_symbols.txt | artifacts/fx_complete_symbols.txt | infra (output) |
| requirements.txt | config/requirements.txt | infra (config) |
| requirements-runtime.txt | config/requirements-runtime.txt | infra (config) |
| requirements-lock.txt | config/requirements-lock.txt | infra (config) |
| pyproject.toml | config/pyproject.toml | infra (config) |
| pytest.ini | config/pytest.ini | infra (config) |
| mypy.ini | config/mypy.ini | infra (config) |
| ruff.toml | config/ruff.toml | infra (config) |
| coverage.ini | config/coverage.ini | infra (config) |
| Dockerfile | octa/infra/deployment/docker/Dockerfile | infra |
| =1.2.0 | docs/version.txt | support (docs) |

## 3) Entry Points

### CLI / Runners
- `octa_training/run_train.py`
- `octa_training/run_daemon.py`
- `scripts/run_octa.py`
- `scripts/run_nexus_paper.py`
- `scripts/octa_autopilot.py`
- `scripts/e2e_orchestrator.py`
- `scripts/train_multiframe_symbol.py`
- `scripts/batch_gate_check_indices_1d.py`
- `scripts/batch_gate_check_indices_multitf.py`
- `scripts/global_gate_diagnose.py`
- `scripts/run_global_gate_1d.py`
- `scripts/run_all_assets.py`
- `scripts/run_dashboard.py`
- `scripts/run_status.py`

### __main__ Entrypoints (selected)
- `scripts/*.py` (many; see rg list)
- `tools/arm_training_from_gate.py`
- `tools/run_secondary_asset_gates.py`
- `tools/train_passed_symbols.py`
- `octa_training/run_train.py`
- `octa_training/run_daemon.py`
- `octa_training/tools/inspect_artifact.py`
- `octa_nexus/paper_boot.py`
- `octa_stream/validate.py`
- `octa_reports/readiness.py`
- `octa_ip/contract_tests.py`

## 4) Import Hotspots (Top 30 Most Imported)
- octa_training.core.config (25)
- octa_training.core.state (20)
- octa_training.core.io_parquet (18)
- octa_ledger.store (18)
- octa_training.core.pipeline (17)
- octa_ledger.core (15)
- core.training_safety_lock (10)
- octa_ops.incidents (10)
- octa_atlas.registry (10)
- octa_training.core.gates (9)
- octa_strategy.lifecycle (9)
- octa_fabric.fingerprint (9)
- octa_ip.module_map (9)
- octa_ledger.events (8)
- octa_core.types (8)
- octa_strategy.state_machine (7)
- octa_sentinel.kill_switch (7)
- octa_capital.aum_state (7)
- dagster (7)
- octa_training.core.metrics_contract (6)
- octa_training.core.features (6)
- octa_training.core.device (6)
- octa_training.core.evaluation (6)
- octa_ops.runbooks (6)
- octa_core.security.audit (6)
- octa_sentinel.engine (6)
- octa_nexus.bus (6)
- scripts.global_gate_diagnose (6)
- octa_training.core.asset_class (5)
- octa_training.core.artifact_io (5)

## 5) Dependency Risks
- Cycles (static): `scripts.hyperparam_search -> scripts.train_and_save -> scripts.hyperparam_search`.
- God modules (fan-out): `octa_training.run_train`, `octa_training.core.pipeline`, `scripts.train_and_save`, `scripts.train_multiframe_symbol`.
- Centralized modules (fan-in): `octa_training.core.config`, `octa_training.core.state`, `octa_training.core.io_parquet`.

## 6) Proposed Micro-Batches (Safe Order, <=30 each; first batch <=15)

Mandatory order group A: docs/, config/, scripts/, tests/.

Batch 1 (<=15 files):
- Move root docs into `docs/`:
  - AGENTS.md
  - README.md
  - README_TRAINING.md
  - README_integration.md
  - MLFLOW_README.md
  - OCTA_AUDIT_REPORT.md
  - OCTA_LIBRARY_MAP.md
  - REPORT_REFACTOR_AUDIT.md
  - REPORT_REFACTOR_AUDIT.json
  - =1.2.0 (to docs/version.txt)

Batch 2 (<=30 files):
- Move root config files into `config/`:
  - requirements.txt, requirements-runtime.txt, requirements-lock.txt
  - pyproject.toml, pytest.ini, mypy.ini, ruff.toml, coverage.ini
- Move root Dockerfile into `octa/infra/deployment/docker/Dockerfile`

Batch 3 (<=30 files):
- Move a first slice of scripts into `scripts/` canonical location with shims:
  - download_aapl_1m.py
  - run_octa.py
  - run_nexus_paper.py
  - run_dashboard.py
  - run_status.py
  - octa_autopilot.py
  - e2e_orchestrator.py
  - train_multiframe_symbol.py
  - batch_gate_check_indices_1d.py
  - batch_gate_check_indices_multitf.py

Note: Larger directories (`docs/`, `configs/`, `scripts/`, `tests/`) will be split into multiple micro-batches to respect file count limits and risk controls.
