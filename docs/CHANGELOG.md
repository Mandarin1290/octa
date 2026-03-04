# Octa Changelog

Automatisch generiert. Alle Änderungen werden hier dokumentiert.

## 2026-03-04


### [2026-03-04 08:21:57] TEST
**Beschreibung:** Code-Änderungen - 2 Code-Dateien - 1 Test-Dateien - (+356/-7 Zeilen)

**Geänderte Dateien:**
- **CODE:** 2 Dateien
  - `octa/core/data/io/io_parquet.py`
  - `octa_ops/autopilot/data_quality.py`
- **TESTS:** 1 Dateien
  - `tests/test_data_schema_normalization.py`


### [2026-03-04 09:14:59] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 4 Code-Dateien - 1 Test-Dateien - (+1016/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 4 Dateien
  - `octa/core/promotion/__init__.py`
  - `octa/core/promotion/promote.py`
  - `octa/core/promotion/promotion_criteria.py`
  - ... und 1 weitere
- **TESTS:** 1 Dateien
  - `tests/test_b4_promotion_gate.py`


### [2026-03-04 10:50:42] TEST
**Beschreibung:** Code-Änderungen - 2 Code-Dateien - 1 Test-Dateien - (+392/-1 Zeilen)

**Geänderte Dateien:**
- **CODE:** 2 Dateien
  - `octa/core/data/io/timeseries_integrity.py`
  - `octa_ops/autopilot/data_quality.py`
- **TESTS:** 1 Dateien
  - `tests/test_futures_1d_integrity_quarantine.py`


### [2026-03-04 11:21:31] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 2 Code-Dateien - 1 Test-Dateien - (+576/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 2 Dateien
  - `octa/core/data/builders/__init__.py`
  - `octa/core/data/builders/futures_1d_regen.py`
- **TESTS:** 1 Dateien
  - `tests/test_futures_1d_regen.py`
- **SCRIPTS:** 1 Dateien
  - `scripts/rebuild_futures_1d.py`


### [2026-03-04 14:57:16] TEST
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - (+54/-3 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/core/orchestration/resources.py`
- **TESTS:** 1 Dateien
  - `tests/test_run_id_uniqueness.py`
- **SCRIPTS:** 2 Dateien
  - `scripts/octa_autopilot.py`
  - `scripts/run_paper_live.py`


### [2026-03-04 15:01:08] TEST
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - (+73/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa_ops/autopilot/registry.py`
- **TESTS:** 1 Dateien
  - `tests/test_registry_wal_mode.py`


### [2026-03-04 15:46:13] TEST
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - (+65/-1 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa_ops/autopilot/paper_runner.py`
- **TESTS:** 1 Dateien
  - `tests/test_disk_budget_wired.py`


## 2026-03-03


### [2026-03-03 06:55:43] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 1 Test-Dateien - (+536/-3 Zeilen)

**Geänderte Dateien:**
- **TESTS:** 1 Dateien
  - `tests/test_strict_cascade.py`
- **CONFIG:** 5 Dateien
  - `configs/autopilot_smoke_etf.yaml`
  - `configs/autopilot_smoke_future.yaml`
  - `configs/autopilot_smoke_fx.yaml`
  - ... und 2 weitere
- **SCRIPTS:** 1 Dateien
  - `scripts/octa_autopilot.py`


### [2026-03-03 20:21:19] TEST
**Beschreibung:** Code-Änderungen - 2 Code-Dateien - 1 Test-Dateien - (+411/-2 Zeilen)

**Geänderte Dateien:**
- **CODE:** 2 Dateien
  - `octa/core/governance/governance_audit.py`
  - `octa/execution/runner.py`
- **TESTS:** 1 Dateien
  - `tests/test_risk_aggregation_wiring.py`


### [2026-03-03 21:01:07] TEST
**Beschreibung:** Code-Änderungen - 4 Code-Dateien - 1 Test-Dateien - (+453/-4 Zeilen)

**Geänderte Dateien:**
- **CODE:** 4 Dateien
  - `octa/execution/runner.py`
  - `octa_vertex/broker/asset_class_router.py`
  - `octa_vertex/broker/ibkr_contract.py`
  - ... und 1 weitere
- **TESTS:** 1 Dateien
  - `tests/test_asset_class_routing.py`


## 2026-03-02


### [2026-03-02 19:40:24] TEST
**Beschreibung:** Code-Änderungen - 1 Test-Dateien - (+15/-0 Zeilen)

**Geänderte Dateien:**
- **TESTS:** 1 Dateien
  - `tests/test_autopilot_dynamic_gate_config.py`
- **SCRIPTS:** 1 Dateien
  - `scripts/octa_autopilot.py`


### [2026-03-02 20:49:47] TEST
**Beschreibung:** Code-Änderungen - 3 Test-Dateien - (+70/-8 Zeilen)

**Geänderte Dateien:**
- **TESTS:** 3 Dateien
  - `configs/autopilot_test_100.yaml`
  - `configs/autopilot_test_50.yaml`
  - `tests/test_multiasset_config_dirs.py`
- **CONFIG:** 2 Dateien
  - `configs/autonomous_paper.yaml`
  - `configs/autopilot_daily.yaml`
- **SCRIPTS:** 1 Dateien
  - `scripts/octa_autopilot.py`


### [2026-03-02 20:51:08] TEST
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - (+79/-1 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa_ops/autopilot/universe.py`
- **TESTS:** 1 Dateien
  - `tests/test_multiasset_etf_wiring.py`
- **SCRIPTS:** 1 Dateien
  - `scripts/octa_autopilot.py`


### [2026-03-02 20:52:26] TEST
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - (+106/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa_ops/autopilot/universe.py`
- **TESTS:** 1 Dateien
  - `tests/test_multiasset_index_wiring.py`
- **SCRIPTS:** 1 Dateien
  - `scripts/octa_autopilot.py`


### [2026-03-02 20:57:08] TEST
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - (+113/-8 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/core/features/transforms/feature_builder.py`
- **TESTS:** 1 Dateien
  - `tests/test_multiasset_altdata_weight_filter.py`


## 2026-03-01


### [2026-03-01 01:04:47] CONFIG
**Beschreibung:** Code-Änderungen - 1 Test-Dateien - (+213/-0 Zeilen)

**Geänderte Dateien:**
- **TESTS:** 1 Dateien
  - `tests/test_altdata_training_offline_mode.py`
- **CONFIG:** 1 Dateien
  - `config/altdat.yaml`


### [2026-03-01 01:04:59] TEST
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - (+450/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/core/data/sources/altdata/snapshot_registry.py`
- **TESTS:** 1 Dateien
  - `tests/test_altdata_snapshot_registry.py`


### [2026-03-01 01:23:51] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 2 Test-Dateien - (+670/-0 Zeilen)

**Geänderte Dateien:**
- **TESTS:** 2 Dateien
  - `configs/autopilot_test_100.yaml`
  - `configs/autopilot_test_50.yaml`
- **SCRIPTS:** 1 Dateien
  - `scripts/run_training_smoke_universe.py`


### [2026-03-01 01:25:02] DOCS
**Beschreibung:** Code-Änderungen - 1 Dokumentations-Dateien - (+132/-0 Zeilen)

**Geänderte Dateien:**
- **DOCS:** 1 Dateien
  - `docs/altdata_training_safety.md`


### [2026-03-01 18:47:08] CHANGE
**Beschreibung:** Code-Änderungen - (+110/-0 Zeilen)

**Geänderte Dateien:**
- **SCRIPTS:** 2 Dateien
  - `scripts/daily_refresh.sh`
  - `scripts/run_altdata_refresh.py`


### [2026-03-01 20:14:53] TEST
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - (+257/-1 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/core/features/features.py`
- **TESTS:** 1 Dateien
  - `tests/test_leakage_audit_altdat.py`


## 2026-02-28


### [2026-02-28 12:14:16] TEST
**Beschreibung:** Code-Änderungen - 2 Code-Dateien - 1 Test-Dateien - (+362/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 2 Dateien
  - `octa_training/core/config.py`
  - `octa_training/core/pipeline.py`
- **TESTS:** 1 Dateien
  - `tests/test_cascade_splits_per_tf.py`
- **CONFIG:** 1 Dateien
  - `configs/dev.yaml`


### [2026-02-28 18:56:12] CHANGE
**Beschreibung:** Code-Änderungen - (+2/-3 Zeilen)

**Geänderte Dateien:**
- **CONFIG:** 1 Dateien
  - `configs/autopilot_daily.yaml`


## 2026-02-27


### [2026-02-27 16:17:50] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 4 Code-Dateien - 2 Test-Dateien - (+650/-9 Zeilen)

**Geänderte Dateien:**
- **CODE:** 4 Dateien
  - `octa/execution/cli/run_pre_execution.py`
  - `octa/execution/pre_execution.py`
  - `octa/execution/runner.py`
  - ... und 1 weitere
- **TESTS:** 2 Dateien
  - `tests/test_paper_runner_pre_execution.py`
  - `tests/test_pre_execution.py`


### [2026-02-27 16:18:57] CHANGE
**Beschreibung:** Code-Änderungen - (+249/-69 Zeilen)

**Geänderte Dateien:**
- **SCRIPTS:** 1 Dateien
  - `scripts/tws_e2e.sh`


### [2026-02-27 16:42:01] TEST
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - (+265/-12 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/execution/pre_execution.py`
- **TESTS:** 1 Dateien
  - `tests/test_pre_execution.py`
- **CONFIG:** 1 Dateien
  - `configs/execution_ibkr.yaml`


### [2026-02-27 18:56:07] CHANGE
**Beschreibung:** Code-Änderungen - (+29/-0 Zeilen)

**Geänderte Dateien:**
- **SCRIPTS:** 1 Dateien
  - `scripts/run_paper_live.py`


### [2026-02-27 19:52:36] CHANGE
**Beschreibung:** Code-Änderungen - (+81/-0 Zeilen)

**Geänderte Dateien:**
- **CONFIG:** 1 Dateien
  - `configs/autopilot_daily.yaml`


## 2026-02-26


### [2026-02-26 19:06:58] TEST
**Beschreibung:** Code-Änderungen - 2 Code-Dateien - 1 Test-Dateien - (+314/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 2 Dateien
  - `octa/execution/capital_state.py`
  - `octa/execution/runner.py`
- **TESTS:** 1 Dateien
  - `tests/test_i5_capital_guard.py`


### [2026-02-26 19:14:32] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 2 Code-Dateien - 1 Test-Dateien - (+723/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 2 Dateien
  - `octa/core/governance/drift_monitor.py`
  - `octa/models/ops/rollback.py`
- **TESTS:** 1 Dateien
  - `tests/test_i6_rollback.py`


### [2026-02-26 19:23:40] TEST
**Beschreibung:** Code-Änderungen - 2 Code-Dateien - 1 Test-Dateien - (+312/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 2 Dateien
  - `octa/execution/runner.py`
  - `octa/execution/tws_probe.py`
- **TESTS:** 1 Dateien
  - `tests/test_i7_tws_probe.py`


### [2026-02-26 19:38:49] TEST
**Beschreibung:** Code-Änderungen - 2 Code-Dateien - 1 Test-Dateien - (+322/-8 Zeilen)

**Geänderte Dateien:**
- **CODE:** 2 Dateien
  - `octa/execution/notifier.py`
  - `octa/execution/runner.py`
- **TESTS:** 1 Dateien
  - `tests/test_i8_alerting.py`
- **CONFIG:** 1 Dateien
  - `configs/policy.yaml`


### [2026-02-26 21:29:31] TEST
**Beschreibung:** Code-Änderungen - 39 Code-Dateien - 3 Test-Dateien - (+85/-68 Zeilen)

**Geänderte Dateien:**
- **CODE:** 39 Dateien
  - `octa/accounting/capital_ledger.py`
  - `octa/core/analytics/attribution.py`
  - `octa/core/analytics/diagnostics.py`
  - ... und 36 weitere
- **TESTS:** 3 Dateien
  - `octa/core/autonomy/tests/test_supervisor_runbooks.py`
  - `tests/test_i8_alerting.py`
  - `tests/test_multiasset_cascade_phase_c.py`
- **CONFIG:** 1 Dateien
  - `pyproject.toml`
- **OTHER:** 1 Dateien
  - `requirements-ops.txt`


### [2026-02-26 21:51:52] TEST
**Beschreibung:** Code-Änderungen - 3 Code-Dateien - 1 Test-Dateien - (+301/-3 Zeilen)

**Geänderte Dateien:**
- **CODE:** 3 Dateien
  - `octa/core/governance/drift_monitor.py`
  - `octa/core/governance/governance_audit.py`
  - `octa_ops/autopilot/paper_runner.py`
- **TESTS:** 1 Dateien
  - `tests/test_drift_monitor_governance.py`


### [2026-02-26 22:15:17] TEST
**Beschreibung:** Code-Änderungen - 1 Test-Dateien - (+2/-1 Zeilen)

**Geänderte Dateien:**
- **TESTS:** 1 Dateien
  - `tests/test_altdat_sidecar_disabled_by_default.py`


### [2026-02-26 22:34:22] TEST
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - (+28/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/support/ops/universe_preflight.py`
- **TESTS:** 1 Dateien
  - `tests/test_universe_preflight.py`


## 2026-02-24


### [2026-02-24 12:54:34] TEST
**Beschreibung:** Code-Änderungen - 4 Code-Dateien - 5 Test-Dateien - (+698/-52 Zeilen)

**Geänderte Dateien:**
- **CODE:** 4 Dateien
  - `octa/core/cascade/adapters.py`
  - `octa/core/cascade/contracts.py`
  - `octa_ops/autopilot/cascade_train.py`
  - ... und 1 weitere
- **TESTS:** 5 Dateien
  - `tests/test_autopilot_cascade_pkl_per_timeframe.py`
  - `tests/test_cascade_invariants.py`
  - `tests/test_cascade_structural_vs_performance.py`
  - ... und 2 weitere


### [2026-02-24 12:56:04] CHANGE
**Beschreibung:** Code-Änderungen - (+157/-153 Zeilen)

**Geänderte Dateien:**
- **SCRIPTS:** 1 Dateien
  - `scripts/tws_e2e.sh`


### [2026-02-24 13:33:09] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 5 Code-Dateien - 5 Test-Dateien - (+1305/-36 Zeilen)

**Geänderte Dateien:**
- **CODE:** 5 Dateien
  - `octa/core/data/storage/artifact_io.py`
  - `octa/core/governance/key_rotation.py`
  - `octa/models/approved_loader.py`
  - ... und 2 weitere
- **TESTS:** 5 Dateien
  - `tests/test_key_rotation_schedule.py`
  - `tests/test_promote_model_atomicity.py`
  - `tests/test_quarantine_emits_governance_event.py`
  - ... und 2 weitere
- **CONFIG:** 1 Dateien
  - `configs/policy.yaml`


### [2026-02-24 18:24:28] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 1 Code-Dateien - 3 Test-Dateien - (+576/-6 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/execution/runner.py`
- **TESTS:** 3 Dateien
  - `tests/test_execution_drift_enforcement.py`
  - `tests/test_execution_nav_reconciliation.py`
  - `tests/test_execution_preflight_enforcement.py`
- **SCRIPTS:** 1 Dateien
  - `scripts/tws_5x.sh`


### [2026-02-24 19:56:17] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 6 Code-Dateien - 4 Test-Dateien - (+2834/-14 Zeilen)

**Geänderte Dateien:**
- **CODE:** 6 Dateien
  - `octa/core/governance/governance_audit.py`
  - `octa/core/governance/immutability_guard.py`
  - `octa/core/governance/lifecycle_controller.py`
  - ... und 3 weitere
- **TESTS:** 4 Dateien
  - `tests/test_immutability_guard_layer.py`
  - `tests/test_lifecycle_controller.py`
  - `tests/test_model_registry.py`
  - ... und 1 weitere
- **SCRIPTS:** 1 Dateien
  - `scripts/octa_autopilot.py`


### [2026-02-24 20:41:38] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 6 Code-Dateien - 1 Test-Dateien - (+905/-8 Zeilen)

**Geänderte Dateien:**
- **CODE:** 6 Dateien
  - `octa/core/governance/drift_monitor.py`
  - `octa/execution/cli/run_execution.py`
  - `octa/execution/cli/run_pre_execution.py`
  - ... und 3 weitere
- **TESTS:** 1 Dateien
  - `tests/test_pre_execution.py`
- **CONFIG:** 1 Dateien
  - `configs/execution_ibkr.yaml`


## 2026-02-23


### [2026-02-23 07:05:57] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 1 Code-Dateien - 1 Test-Dateien - (+784/-3 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/support/ibkr_credentials.py`
- **TESTS:** 1 Dateien
  - `tests/test_ibkr_credentials.py`
- **SCRIPTS:** 2 Dateien
  - `scripts/tws_e2e.sh`
  - `scripts/tws_x11_autologin_chain.py`


### [2026-02-23 20:34:40] CONFIG
**Beschreibung:** Code-Änderungen - 26 Code-Dateien - 3 Test-Dateien - (+4785/-294 Zeilen)

**Geänderte Dateien:**
- **CODE:** 26 Dateien
  - `octa/core/cascade/registry.py`
  - `octa/core/data/sources/altdata/sidecar.py`
  - `octa/core/execution/state.py`
  - ... und 23 weitere
- **TESTS:** 3 Dateien
  - `tests/test_multiasset_cascade_phase_c.py`
  - `tests/test_multiasset_cascade_phase_d.py`
  - `tests/test_multiasset_cascade_phase_e.py`
- **CONFIG:** 2 Dateien
  - `config/altdat.yaml`
  - `config/release.yaml`
- **SCRIPTS:** 3 Dateien
  - `scripts/aggregate_option_snapshots.py`
  - `scripts/train_options_lstm.py`
  - `scripts/tws_e2e.sh`


## 2026-02-22


### [2026-02-22 13:00:58] TEST
**Beschreibung:** Code-Änderungen - 6 Code-Dateien - 5 Test-Dateien - (+5610/-211 Zeilen)

**Geänderte Dateien:**
- **CODE:** 6 Dateien
  - `octa/core/gates/training_selection_gate.py`
  - `octa_ops/autopilot/cascade_train.py`
  - `octa_ops/autopilot/data_quality.py`
  - ... und 3 weitere
- **TESTS:** 5 Dateien
  - `tests/test_autopilot_data_quality_basic.py`
  - `tests/test_autopilot_dynamic_gate_config.py`
  - `tests/test_autopilot_structural_audit.py`
  - ... und 2 weitere
- **CONFIG:** 2 Dateien
  - `configs/autonomous_paper.yaml`
  - `configs/execution_ibkr.yaml`
- **SCRIPTS:** 3 Dateien
  - `scripts/octa_autopilot.py`
  - `scripts/octa_smoke_chain.py`
  - `scripts/tws_x11_autologin_chain.py`
- **OTHER:** 2 Dateien
  - `octa/support/x11/tws_popup_controller.sh`
  - `run_tws_autologin.sh`


### [2026-02-22 13:05:39] CHANGE
**Beschreibung:** Code-Änderungen - (+91/-0 Zeilen)

**Geänderte Dateien:**
- **OTHER:** 2 Dateien
  - `.gitignore`
  - `run_tws_autologin.sh.example`


### [2026-02-22 15:25:22] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 1 Code-Dateien - 1 Test-Dateien - (+1615/-7 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/support/x11/popup_rules.py`
- **TESTS:** 1 Dateien
  - `tests/test_tws_popup_rules.py`
- **SCRIPTS:** 2 Dateien
  - `scripts/tws_popup_smoke_harness.py`
  - `scripts/tws_x11_autologin_chain.py`
- **OTHER:** 1 Dateien
  - `octa/support/x11/tws_popup_controller.sh`


### [2026-02-22 15:45:07] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 2 Code-Dateien - 1 Test-Dateien - (+537/-4 Zeilen)

**Geänderte Dateien:**
- **CODE:** 2 Dateien
  - `octa/support/x11/__init__.py`
  - `octa/support/x11/x11_actions.py`
- **TESTS:** 1 Dateien
  - `tests/test_tws_x11_close_ladder.py`
- **SCRIPTS:** 1 Dateien
  - `scripts/tws_x11_autologin_chain.py`


## 2026-02-20


### [2026-02-20 13:46:42] CHANGE
**Beschreibung:** Code-Änderungen - 3 Code-Dateien - (+157/-11 Zeilen)

**Geänderte Dateien:**
- **CODE:** 3 Dateien
  - `octa/support/ops/v000_full_universe_cascade_train.py`
  - `octa_core/control_plane/api.py`
  - `octa_ops/autopilot/universe.py`
- **CONFIG:** 1 Dateien
  - `octa_core/config/octa_features.yaml`
- **SCRIPTS:** 2 Dateien
  - `scripts/run_octa.py`
  - `scripts/tws_x11_autologin_chain.py`
- **OTHER:** 1 Dateien
  - `systemd/octa-autologin.service`


### [2026-02-20 18:16:57] DOCS
**Beschreibung:** Code-Änderungen - 1 Dokumentations-Dateien - (+164/-0 Zeilen)

**Geänderte Dateien:**
- **DOCS:** 1 Dateien
  - `docs/RUNBOOK_HOST_X11_IBKR_VERIFY.md`


## 2026-02-19


### [2026-02-19 18:06:48] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 18 Code-Dateien - 2 Test-Dateien - 1 Dokumentations-Dateien - (+2021/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 18 Dateien
  - `octa/os/__init__.py`
  - `octa/os/capabilities.py`
  - `octa/os/eligibility.py`
  - ... und 15 weitere
- **TESTS:** 2 Dateien
  - `tests/test_octa_os_brain.py`
  - `tests/test_octa_os_eligibility.py`
- **DOCS:** 1 Dateien
  - `docs/OCTA_OS.md`
- **CONFIG:** 1 Dateien
  - `configs/policy.yaml`
- **SCRIPTS:** 3 Dateien
  - `scripts/octa_os_gate.sh`
  - `scripts/octa_os_start.py`
  - `scripts/octa_os_stop.py`
- **OTHER:** 1 Dateien
  - `systemd/octa-os.service.example`


### [2026-02-19 18:07:03] CHANGE
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - (+229/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/support/branding.py`
- **SCRIPTS:** 1 Dateien
  - `scripts/brand_guard.py`


### [2026-02-19 18:07:15] DOCS
**Beschreibung:** Code-Änderungen - 2 Dokumentations-Dateien - (+332/-35 Zeilen)

**Geänderte Dateien:**
- **DOCS:** 2 Dateien
  - `docs/IBKR_X11_AUTOLOGIN.md`
  - `docs/SYSTEMD_AUTOSTART.md`
- **CONFIG:** 1 Dateien
  - `configs/execution_ibkr.yaml`
- **SCRIPTS:** 2 Dateien
  - `scripts/octa_autopilot.py`
  - `scripts/octa_smoke_chain.py`


### [2026-02-19 18:07:22] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 1 Code-Dateien - 2 Test-Dateien - (+1724/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/execution/x11_preflight.py`
- **TESTS:** 2 Dateien
  - `tests/test_ibkr_supervisor.py`
  - `tests/test_x11_preflight.py`
- **SCRIPTS:** 2 Dateien
  - `scripts/octa_ibkr_supervisor.py`
  - `scripts/tws_x11_autologin_chain.py`


## 2026-02-18


### [2026-02-18 18:15:35] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 19 Code-Dateien - 9 Test-Dateien - 1 Dokumentations-Dateien - (+3753/-16 Zeilen)

**Geänderte Dateien:**
- **CODE:** 19 Dateien
  - `octa/accounting/__init__.py`
  - `octa/accounting/capital_ledger.py`
  - `octa/accounting/ops/__init__.py`
  - ... und 16 weitere
- **TESTS:** 9 Dateien
  - `tests/test_approved_model_loader.py`
  - `tests/test_artifact_signing.py`
  - `tests/test_capital_ledger.py`
  - ... und 6 weitere
- **DOCS:** 1 Dateien
  - `docs/GOVERNANCE_POLICY.md`
- **OTHER:** 1 Dateien
  - `.gitignore`


### [2026-02-18 19:35:07] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 1 Dokumentations-Dateien - (+745/-17 Zeilen)

**Geänderte Dateien:**
- **DOCS:** 1 Dateien
  - `docs/IBKR_X11_AUTOLOGIN.md`
- **SCRIPTS:** 1 Dateien
  - `scripts/octa_ibkr_autologin_healthcheck.sh`
- **OTHER:** 1 Dateien
  - `systemd/octa-autologin.service`


### [2026-02-18 19:55:49] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - (+1438/-0 Zeilen)

**Geänderte Dateien:**
- **SCRIPTS:** 1 Dateien
  - `scripts/octa_ibkr_autologin_watch.py`


### [2026-02-18 20:01:18] DOCS
**Beschreibung:** Code-Änderungen - 1 Dokumentations-Dateien - (+119/-5 Zeilen)

**Geänderte Dateien:**
- **DOCS:** 1 Dateien
  - `docs/IBKR_X11_AUTOLOGIN.md`
- **SCRIPTS:** 1 Dateien
  - `scripts/octa_ibkr_autologin_watch.py`


### [2026-02-18 20:09:17] DOCS
**Beschreibung:** Code-Änderungen - 1 Test-Dateien - 1 Dokumentations-Dateien - (+324/-0 Zeilen)

**Geänderte Dateien:**
- **TESTS:** 1 Dateien
  - `scripts/octa_ibkr_autologin_e2e_test.sh`
- **DOCS:** 1 Dateien
  - `docs/IBKR_X11_AUTOLOGIN.md`


### [2026-02-18 22:09:01] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 3 Code-Dateien - 1 Test-Dateien - (+951/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 3 Dateien
  - `octa/core/data/quality/series_validator.py`
  - `octa/execution/risk_fail_closed.py`
  - `octa/execution/risk_fail_closed_harness.py`
- **TESTS:** 1 Dateien
  - `tests/test_risk_fail_closed.py`
- **CONFIG:** 1 Dateien
  - `pyproject.toml`
- **SCRIPTS:** 1 Dateien
  - `scripts/octa_smoke_chain.py`
- **OTHER:** 1 Dateien
  - `octa/var/releases/v0.0.0/manifest.json`


### [2026-02-18 22:11:40] DOCS
**Beschreibung:** Code-Änderungen - 1 Dokumentations-Dateien - (+70/-0 Zeilen)

**Geänderte Dateien:**
- **DOCS:** 1 Dateien
  - `docs/RELEASE_CUT_V0_0_0.md`


## 2026-02-17


### [2026-02-17 14:48:56] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 6 Code-Dateien - 5 Test-Dateien - 1 Dokumentations-Dateien - (+2042/-12 Zeilen)

**Geänderte Dateien:**
- **CODE:** 6 Dateien
  - `octa/execution/ibkr_autologin_store.py`
  - `octa/execution/ibkr_runtime.py`
  - `octa/execution/ibkr_x11_autologin.py`
  - ... und 3 weitere
- **TESTS:** 5 Dateien
  - `tests/test_cascade_structural_vs_performance.py`
  - `tests/test_ibkr_x11_autologin_logic.py`
  - `tests/test_ibkr_x11_autologin_store.py`
  - ... und 2 weitere
- **DOCS:** 1 Dateien
  - `docs/SYSTEMD_AUTOSTART.md`
- **SCRIPTS:** 7 Dateien
  - `scripts/octa_autologin_bootstrap.sh`
  - `scripts/octa_health_watchdog.py`
  - `scripts/octa_ibkr_bootstrap.sh`
  - ... und 4 weitere
- **OTHER:** 5 Dateien
  - `systemd/octa-autologin.service`
  - `systemd/octa-ibkr.service`
  - `systemd/octa-v000.service`
  - ... und 2 weitere


### [2026-02-17 15:17:38] DOCS
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - 1 Dokumentations-Dateien - (+186/-36 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/support/ops/v000_full_universe_cascade_train.py`
- **TESTS:** 1 Dateien
  - `tests/test_systemd_smoke.py`
- **DOCS:** 1 Dateien
  - `docs/SYSTEMD_AUTOSTART.md`
- **SCRIPTS:** 5 Dateien
  - `scripts/octa_autologin_bootstrap.sh`
  - `scripts/octa_health_watchdog.py`
  - `scripts/octa_ibkr_bootstrap.sh`
  - ... und 2 weitere
- **OTHER:** 4 Dateien
  - `systemd/octa-autologin.service`
  - `systemd/octa-ibkr.service`
  - `systemd/octa-v000.service`
  - ... und 1 weitere


### [2026-02-17 15:27:31] DOCS
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - 1 Dokumentations-Dateien - (+111/-48 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/support/ops/v000_full_universe_cascade_train.py`
- **TESTS:** 1 Dateien
  - `tests/test_systemd_smoke.py`
- **DOCS:** 1 Dateien
  - `docs/SYSTEMD_AUTOSTART.md`
- **SCRIPTS:** 1 Dateien
  - `scripts/octa_x11_bootstrap.sh`
- **OTHER:** 1 Dateien
  - `systemd/octa-x11.service`


### [2026-02-17 18:05:36] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 1 Code-Dateien - 2 Test-Dateien - 1 Dokumentations-Dateien - (+1360/-22 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/execution/ibkr_x11_login.py`
- **TESTS:** 2 Dateien
  - `tests/test_bootstrap_no_bare_python.py`
  - `tests/test_ibkr_x11_login_steps.py`
- **DOCS:** 1 Dateien
  - `docs/IBKR_X11_AUTOLOGIN.md`
- **SCRIPTS:** 2 Dateien
  - `scripts/octa_autologin_bootstrap.sh`
  - `scripts/octa_ibkr_bootstrap.sh`


## 2026-02-15


### [2026-02-15 14:40:09] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 1 Code-Dateien - 1 Test-Dateien - (+519/-0 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/support/ops/v000_finish_paper_ready_local_only.py`
- **TESTS:** 1 Dateien
  - `tests/test_v000_finish_paper_ready_local_only_synth.py`


## 2026-02-13


### [2026-02-13 06:16:05] CHANGE
**Beschreibung:** Code-Änderungen - 5 Code-Dateien - (+270/-39 Zeilen)

**Geänderte Dateien:**
- **CODE:** 5 Dateien
  - `octa/core/data/sources/altdata/cache.py`
  - `octa/core/features/features.py`
  - `octa/core/features/transforms/feature_builder.py`
  - ... und 2 weitere
- **CONFIG:** 1 Dateien
  - `octa_training/config/training.yaml`


### [2026-02-13 13:23:07] CHANGE
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - (+5/-1 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/core/features/features.py`


### [2026-02-13 16:40:51] BUGFIX
**Beschreibung:** Fehlerbehebung - 25 Code-Dateien - 13 Test-Dateien - 2 Dokumentations-Dateien - (+408429/-481537961 Zeilen)

**Geänderte Dateien:**
- **CODE:** 25 Dateien
  - `octa/core/eligibility/__init__.py`
  - `octa/core/eligibility/filter.py`
  - `octa/core/execution/costs/model.py`
  - ... und 22 weitere
- **TESTS:** 13 Dateien
  - `tests/test_carry_config_and_rates.py`
  - `tests/test_carry_risk_gate.py`
  - `tests/test_carry_signals.py`
  - ... und 10 weitere
- **DOCS:** 2 Dateien
  - `ARCHITECTURE_v0_0_0.md`
  - `README_v0_0_0.md`
- **DATA:** 9608 Dateien
  - `data/altdat/meta/features_AACBR_1D_altdat_20260211T211304Z_AACBR_1D.json`
  - `data/altdat/meta/features_AACBR_1D_altdat_20260211T211305Z_AACBR_1D.json`
  - `data/altdat/meta/features_AACBR_1D_altdat_20260211T211413Z_AACBR_1D.json`
  - ... und 9605 weitere
- **OTHER:** 9 Dateien
  - `logs/octa_training.jsonl`
  - `raw_equities_only/equities`
  - `state.bak_20260211T062044Z/_cascade_hf/state.db`
  - ... und 6 weitere


### [2026-02-13 16:47:38] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - (+3412/-0 Zeilen)

**Geänderte Dateien:**
- **DATA:** 64 Dateien
  - `data/altdat/meta/features_AVAH_1D_altdat_20260213T153616Z_AVAH_1D.json`
  - `data/altdat/meta/features_AVAH_1D_altdat_20260213T153618Z_AVAH_1D.json`
  - `data/altdat/meta/features_AVAH_1D_altdat_20260213T153619Z_AVAH_1D.json`
  - ... und 61 weitere


## 2026-02-11



### [2026-02-11 20:11:48] CHANGE
**Beschreibung:** Code-Änderungen - (+4/-0 Zeilen)

**Geänderte Dateien:**
- **OTHER:** 2 Dateien
  - `.gitignore`
  - `state/state.db`


### [2026-02-11 20:49:27] TEST
**Beschreibung:** Code-Änderungen - 9 Code-Dateien - 3 Test-Dateien - (+2268/-108 Zeilen)

**Geänderte Dateien:**
- **CODE:** 9 Dateien
  - `octa/core/data/sources/altdata/orchestrator.py`
  - `octa/core/features/features.py`
  - `octa/core/features/transforms/feature_builder.py`
  - ... und 6 weitere
- **TESTS:** 3 Dateien
  - `tests/test_altdata_fail_soft_deterministic.py`
  - `tests/test_monte_carlo_mandatory.py`
  - `tests/test_run_full_cascade_training_from_parquets.py`
- **CONFIG:** 1 Dateien
  - `octa_training/config/training.yaml`


### [2026-02-11 21:17:47] FEATURE
**Beschreibung:** Neue Funktionalität hinzugefügt - 6 Code-Dateien - 2 Test-Dateien - (+1077/-37 Zeilen)

**Geänderte Dateien:**
- **CODE:** 6 Dateien
  - `octa/support/ops/run_full_cascade_training_from_parquets.py`
  - `octa_ops/autopilot/cascade_train.py`
  - `octa_training/core/gates.py`
  - ... und 3 weitere
- **TESTS:** 2 Dateien
  - `tests/test_institutional_gates_mandatory.py`
  - `tests/test_run_full_cascade_training_from_parquets.py`


### [2026-02-11 21:26:29] TEST
**Beschreibung:** Code-Änderungen - 2 Code-Dateien - 2 Test-Dateien - (+420/-2 Zeilen)

**Geänderte Dateien:**
- **CODE:** 2 Dateien
  - `octa/core/data/io/io_parquet.py`
  - `octa/support/ops/build_raw_tree_from_flat_parquets.py`
- **TESTS:** 2 Dateien
  - `tests/test_build_raw_tree_from_flat_parquets.py`
  - `tests/test_io_parquet_recursive_discovery.py`


### [2026-02-11 21:55:33] CONFIG
**Beschreibung:** Code-Änderungen - 3 Code-Dateien - 7 Test-Dateien - (+232/-49 Zeilen)

**Geänderte Dateien:**
- **CODE:** 3 Dateien
  - `octa/support/ops/run_full_cascade_training_from_parquets.py`
  - `octa_ops/autopilot/cascade_train.py`
  - `octa_training/core/pipeline.py`
- **TESTS:** 7 Dateien
  - `config/pytest.ini`
  - `octa/core/system_tests/test_full_autonomy_parquet_e2e.py`
  - `octa/core/system_tests/test_full_autonomy_parquet_e2e_5symbols.py`
  - ... und 4 weitere
- **OTHER:** 1 Dateien
  - `octa/support/ops/run_evidence_smoke.sh`


### [2026-02-11 22:01:28] TEST
**Beschreibung:** Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - (+413/-170 Zeilen)

**Geänderte Dateien:**
- **CODE:** 1 Dateien
  - `octa/support/ops/build_raw_tree_from_flat_parquets.py`
- **TESTS:** 1 Dateien
  - `tests/test_build_raw_tree_from_flat_parquets.py`

