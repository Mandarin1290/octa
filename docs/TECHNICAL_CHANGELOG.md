# Technical Changelog

Detaillierte technische Änderungen. Auto-generiert.


## [2026-02-26 19:14:32] FEATURE
Neue Funktionalität hinzugefügt - 2 Code-Dateien - 1 Test-Dateien - (+723/-0 Zeilen)

**Statistics:**
- Files Changed: 3
- Lines Added: +723
- Lines Deleted: -0
- Net Change: +723

**File-Level Changes:**
```
+   19 -    0  octa/core/governance/drift_monitor.py
+  241 -    0  octa/models/ops/rollback.py
+  463 -    0  tests/test_i6_rollback.py
```


## [2026-02-26 19:06:58] TEST
Code-Änderungen - 2 Code-Dateien - 1 Test-Dateien - (+314/-0 Zeilen)

**Statistics:**
- Files Changed: 3
- Lines Added: +314
- Lines Deleted: -0
- Net Change: +314

**File-Level Changes:**
```
+   57 -    0  octa/execution/capital_state.py
+   28 -    0  octa/execution/runner.py
+  229 -    0  tests/test_i5_capital_guard.py
```


## [2026-02-24 20:41:38] FEATURE
Neue Funktionalität hinzugefügt - 6 Code-Dateien - 1 Test-Dateien - (+905/-8 Zeilen)

**Statistics:**
- Files Changed: 8
- Lines Added: +905
- Lines Deleted: -8
- Net Change: +897

**File-Level Changes:**
```
+   21 -    0  configs/execution_ibkr.yaml
+   29 -    5  octa/core/governance/drift_monitor.py
+    5 -    0  octa/execution/cli/run_execution.py
+   55 -    0  octa/execution/cli/run_pre_execution.py
+  396 -    0  octa/execution/pre_execution.py
+   36 -    1  octa/execution/runner.py
+   49 -    2  octa/os/os_brain.py
+  314 -    0  tests/test_pre_execution.py
```


## [2026-02-24 19:56:17] FEATURE
Neue Funktionalität hinzugefügt - 6 Code-Dateien - 4 Test-Dateien - (+2834/-14 Zeilen)

**Statistics:**
- Files Changed: 11
- Lines Added: +2834
- Lines Deleted: -14
- Net Change: +2820

**File-Level Changes:**
```
+    2 -    0  octa/core/governance/governance_audit.py
+  155 -    0  octa/core/governance/immutability_guard.py
+  502 -    0  octa/core/governance/lifecycle_controller.py
+  298 -    0  octa/core/governance/model_registry.py
+  215 -    0  octa/core/governance/promotion_engine.py
+   96 -   12  octa_ops/autopilot/registry.py
+  189 -    2  scripts/octa_autopilot.py
+  198 -    0  tests/test_immutability_guard_layer.py
+  579 -    0  tests/test_lifecycle_controller.py
+  110 -    0  tests/test_model_registry.py
+  490 -    0  tests/test_promotion_engine.py
```


## [2026-02-24 18:24:28] FEATURE
Neue Funktionalität hinzugefügt - 1 Code-Dateien - 3 Test-Dateien - (+576/-6 Zeilen)

**Statistics:**
- Files Changed: 5
- Lines Added: +576
- Lines Deleted: -6
- Net Change: +570

**File-Level Changes:**
```
+  227 -    6  octa/execution/runner.py
+    8 -    0  scripts/tws_5x.sh
+  127 -    0  tests/test_execution_drift_enforcement.py
+  136 -    0  tests/test_execution_nav_reconciliation.py
+   78 -    0  tests/test_execution_preflight_enforcement.py
```


## [2026-02-24 13:33:09] FEATURE
Neue Funktionalität hinzugefügt - 5 Code-Dateien - 5 Test-Dateien - (+1305/-36 Zeilen)

**Statistics:**
- Files Changed: 11
- Lines Added: +1305
- Lines Deleted: -36
- Net Change: +1269

**File-Level Changes:**
```
+    5 -    0  configs/policy.yaml
+   38 -    1  octa/core/data/storage/artifact_io.py
+  184 -    0  octa/core/governance/key_rotation.py
+   13 -    1  octa/models/approved_loader.py
+   52 -   32  octa/models/ops/promote.py
+   44 -    2  octa_ops/autopilot/registry.py
+  313 -    0  tests/test_key_rotation_schedule.py
+  139 -    0  tests/test_promote_model_atomicity.py
+  180 -    0  tests/test_quarantine_emits_governance_event.py
+  186 -    0  tests/test_registry_schema_migration.py
+  151 -    0  tests/test_sha256_mismatch_rejects_load.py
```


## [2026-02-24 12:56:04] CHANGE
Code-Änderungen - (+157/-153 Zeilen)

**Statistics:**
- Files Changed: 1
- Lines Added: +157
- Lines Deleted: -153
- Net Change: +4

**File-Level Changes:**
```
+  157 -  153  scripts/tws_e2e.sh
```


## [2026-02-24 12:54:34] TEST
Code-Änderungen - 4 Code-Dateien - 5 Test-Dateien - (+698/-52 Zeilen)

**Statistics:**
- Files Changed: 9
- Lines Added: +698
- Lines Deleted: -52
- Net Change: +646

**File-Level Changes:**
```
+    3 -    0  octa/core/cascade/adapters.py
+    3 -    0  octa/core/cascade/contracts.py
+   53 -   19  octa_ops/autopilot/cascade_train.py
+   42 -    3  octa_training/core/splits.py
+    4 -    0  tests/test_autopilot_cascade_pkl_per_timeframe.py
+  565 -    0  tests/test_cascade_invariants.py
+   11 -   15  tests/test_cascade_structural_vs_performance.py
+   11 -   12  tests/test_multiasset_cascade_phase_d.py
+    6 -    3  tests/test_multiasset_cascade_phase_e.py
```


## [2026-02-23 20:34:40] CONFIG
Code-Änderungen - 26 Code-Dateien - 3 Test-Dateien - (+4785/-294 Zeilen)

**Statistics:**
- Files Changed: 34
- Lines Added: +4785
- Lines Deleted: -294
- Net Change: +4491

**File-Level Changes:**
```
+   13 -    5  config/altdat.yaml
+  183 -    0  config/release.yaml
+   29 -    0  octa/core/cascade/registry.py
+   11 -    1  octa/core/data/sources/altdata/sidecar.py
+  134 -    3  octa/core/execution/state.py
+   77 -    0  octa/core/features/altdata/basis_features.py
+   61 -    0  octa/core/features/altdata/builders.py
+   39 -    0  octa/core/features/altdata/cot_features.py
+   36 -    0  octa/core/features/altdata/eco_calendar_features.py
+   39 -    0  octa/core/features/altdata/funding_rate_features.py
+   43 -    0  octa/core/features/altdata/greeks_features.py
+   40 -    0  octa/core/features/altdata/iv_surface_features.py
+   58 -    0  octa/core/features/altdata/onchain_features.py
+   43 -    0  octa/core/features/altdata/pack.py
+   51 -    0  octa/core/features/altdata/registry.py
+  216 -    0  octa/core/features/altdata/weighting.py
+   12 -    1  octa/core/portfolio/correlation.py
+    0 -    0  octa/support/cli/__init__.py
+    6 -    0  octa/support/cli/banner.py
+    5 -    0  octa/support/ops/run_institutional_train.py
... and 14 more files
```


## [2026-02-23 07:05:57] FEATURE
Neue Funktionalität hinzugefügt - 1 Code-Dateien - 1 Test-Dateien - (+784/-3 Zeilen)

**Statistics:**
- Files Changed: 4
- Lines Added: +784
- Lines Deleted: -3
- Net Change: +781

**File-Level Changes:**
```
+  159 -    0  octa/support/ibkr_credentials.py
+  228 -    0  scripts/tws_e2e.sh
+   38 -    3  scripts/tws_x11_autologin_chain.py
+  359 -    0  tests/test_ibkr_credentials.py
```


## [2026-02-22 15:45:07] FEATURE
Neue Funktionalität hinzugefügt - 2 Code-Dateien - 1 Test-Dateien - (+537/-4 Zeilen)

**Statistics:**
- Files Changed: 4
- Lines Added: +537
- Lines Deleted: -4
- Net Change: +533

**File-Level Changes:**
```
+    1 -    0  octa/support/x11/__init__.py
+  141 -    0  octa/support/x11/x11_actions.py
+   22 -    4  scripts/tws_x11_autologin_chain.py
+  373 -    0  tests/test_tws_x11_close_ladder.py
```


## [2026-02-22 15:25:22] FEATURE
Neue Funktionalität hinzugefügt - 1 Code-Dateien - 1 Test-Dateien - (+1615/-7 Zeilen)

**Statistics:**
- Files Changed: 5
- Lines Added: +1615
- Lines Deleted: -7
- Net Change: +1608

**File-Level Changes:**
```
+  457 -    0  octa/support/x11/popup_rules.py
+   13 -    5  octa/support/x11/tws_popup_controller.sh
+  411 -    0  scripts/tws_popup_smoke_harness.py
+   26 -    2  scripts/tws_x11_autologin_chain.py
+  708 -    0  tests/test_tws_popup_rules.py
```


## [2026-02-22 13:05:39] CHANGE
Code-Änderungen - (+91/-0 Zeilen)

**Statistics:**
- Files Changed: 2
- Lines Added: +91
- Lines Deleted: -0
- Net Change: +91

**File-Level Changes:**
```
+    1 -    0  .gitignore
+   90 -    0  run_tws_autologin.sh.example
```


## [2026-02-22 13:00:58] TEST
Code-Änderungen - 6 Code-Dateien - 5 Test-Dateien - (+5610/-211 Zeilen)

**Statistics:**
- Files Changed: 18
- Lines Added: +5610
- Lines Deleted: -211
- Net Change: +5399

**File-Level Changes:**
```
+   36 -    1  configs/autonomous_paper.yaml
+    1 -    1  configs/execution_ibkr.yaml
+  318 -    0  octa/core/gates/training_selection_gate.py
+  423 -    0  octa/support/x11/tws_popup_controller.sh
+   80 -    0  octa_ops/autopilot/cascade_train.py
+   25 -    4  octa_ops/autopilot/data_quality.py
+   10 -    0  octa_training/core/metrics_contract.py
+   27 -    0  octa_training/core/models.py
+   11 -    1  octa_training/core/pipeline.py
+   90 -    0  run_tws_autologin.sh
+ 1960 -   63  scripts/octa_autopilot.py
+   30 -    8  scripts/octa_smoke_chain.py
+ 1728 -  133  scripts/tws_x11_autologin_chain.py
+   57 -    0  tests/test_autopilot_data_quality_basic.py
+  106 -    0  tests/test_autopilot_dynamic_gate_config.py
+  573 -    0  tests/test_autopilot_structural_audit.py
+  116 -    0  tests/test_training_selection_gate.py
+   19 -    0  tests/test_training_selection_gate_boundaries.py
```


## [2026-02-20 18:16:57] DOCS
Code-Änderungen - 1 Dokumentations-Dateien - (+164/-0 Zeilen)

**Statistics:**
- Files Changed: 1
- Lines Added: +164
- Lines Deleted: -0
- Net Change: +164

**File-Level Changes:**
```
+  164 -    0  docs/RUNBOOK_HOST_X11_IBKR_VERIFY.md
```


## [2026-02-20 13:46:42] CHANGE
Code-Änderungen - 3 Code-Dateien - (+157/-11 Zeilen)

**Statistics:**
- Files Changed: 7
- Lines Added: +157
- Lines Deleted: -11
- Net Change: +146

**File-Level Changes:**
```
+    1 -    4  octa/support/ops/v000_full_universe_cascade_train.py
+    4 -    0  octa_core/config/octa_features.yaml
+   71 -    1  octa_core/control_plane/api.py
+   26 -    0  octa_ops/autopilot/universe.py
+    9 -    4  scripts/run_octa.py
+   42 -    0  scripts/tws_x11_autologin_chain.py
+    4 -    2  systemd/octa-autologin.service
```


## [2026-02-19 18:07:22] FEATURE
Neue Funktionalität hinzugefügt - 1 Code-Dateien - 2 Test-Dateien - (+1724/-0 Zeilen)

**Statistics:**
- Files Changed: 5
- Lines Added: +1724
- Lines Deleted: -0
- Net Change: +1724

**File-Level Changes:**
```
+  347 -    0  octa/execution/x11_preflight.py
+  714 -    0  scripts/octa_ibkr_supervisor.py
+  508 -    0  scripts/tws_x11_autologin_chain.py
+  119 -    0  tests/test_ibkr_supervisor.py
+   36 -    0  tests/test_x11_preflight.py
```


## [2026-02-19 18:07:15] DOCS
Code-Änderungen - 2 Dokumentations-Dateien - (+332/-35 Zeilen)

**Statistics:**
- Files Changed: 5
- Lines Added: +332
- Lines Deleted: -35
- Net Change: +297

**File-Level Changes:**
```
+   50 -    0  configs/execution_ibkr.yaml
+   48 -    0  docs/IBKR_X11_AUTOLOGIN.md
+   39 -    0  docs/SYSTEMD_AUTOSTART.md
+  132 -   20  scripts/octa_autopilot.py
+   63 -   15  scripts/octa_smoke_chain.py
```


## [2026-02-19 18:07:03] CHANGE
Code-Änderungen - 1 Code-Dateien - (+229/-0 Zeilen)

**Statistics:**
- Files Changed: 2
- Lines Added: +229
- Lines Deleted: -0
- Net Change: +229

**File-Level Changes:**
```
+  102 -    0  octa/support/branding.py
+  127 -    0  scripts/brand_guard.py
```


## [2026-02-19 18:06:48] FEATURE
Neue Funktionalität hinzugefügt - 18 Code-Dateien - 2 Test-Dateien - 1 Dokumentations-Dateien - (+2021/-0 Zeilen)

**Statistics:**
- Files Changed: 26
- Lines Added: +2021
- Lines Deleted: -0
- Net Change: +2021

**File-Level Changes:**
```
+   54 -    0  configs/policy.yaml
+   74 -    0  docs/OCTA_OS.md
+    3 -    0  octa/os/__init__.py
+   42 -    0  octa/os/capabilities.py
+  108 -    0  octa/os/eligibility.py
+   28 -    0  octa/os/evidence.py
+  452 -    0  octa/os/os_brain.py
+  117 -    0  octa/os/policy_loader.py
+  137 -    0  octa/os/runbooks.py
+  151 -    0  octa/os/sensors.py
+   16 -    0  octa/os/services/__init__.py
+   34 -    0  octa/os/services/alerts_service.py
+   37 -    0  octa/os/services/base.py
+   46 -    0  octa/os/services/broker_service.py
+   16 -    0  octa/os/services/dashboard_service.py
+   55 -    0  octa/os/services/execution_service.py
+   62 -    0  octa/os/services/training_service.py
+   82 -    0  octa/os/state_store.py
+   79 -    0  octa/os/two_phase_commit.py
+   60 -    0  octa/os/utils.py
... and 6 more files
```


## [2026-02-18 22:11:40] DOCS
Code-Änderungen - 1 Dokumentations-Dateien - (+70/-0 Zeilen)

**Statistics:**
- Files Changed: 1
- Lines Added: +70
- Lines Deleted: -0
- Net Change: +70

**File-Level Changes:**
```
+   70 -    0  docs/RELEASE_CUT_V0_0_0.md
```


## [2026-02-18 22:09:01] FEATURE
Neue Funktionalität hinzugefügt - 3 Code-Dateien - 1 Test-Dateien - (+951/-0 Zeilen)

**Statistics:**
- Files Changed: 7
- Lines Added: +951
- Lines Deleted: -0
- Net Change: +951

**File-Level Changes:**
```
+   87 -    0  octa/core/data/quality/series_validator.py
+  131 -    0  octa/execution/risk_fail_closed.py
+   80 -    0  octa/execution/risk_fail_closed_harness.py
+   13 -    0  octa/var/releases/v0.0.0/manifest.json
+    6 -    0  pyproject.toml
+  598 -    0  scripts/octa_smoke_chain.py
+   36 -    0  tests/test_risk_fail_closed.py
```


## [2026-02-18 20:09:17] DOCS
Code-Änderungen - 1 Test-Dateien - 1 Dokumentations-Dateien - (+324/-0 Zeilen)

**Statistics:**
- Files Changed: 2
- Lines Added: +324
- Lines Deleted: -0
- Net Change: +324

**File-Level Changes:**
```
+   38 -    0  docs/IBKR_X11_AUTOLOGIN.md
+  286 -    0  scripts/octa_ibkr_autologin_e2e_test.sh
```


## [2026-02-18 20:01:18] DOCS
Code-Änderungen - 1 Dokumentations-Dateien - (+119/-5 Zeilen)

**Statistics:**
- Files Changed: 2
- Lines Added: +119
- Lines Deleted: -5
- Net Change: +114

**File-Level Changes:**
```
+   81 -    0  docs/IBKR_X11_AUTOLOGIN.md
+   38 -    5  scripts/octa_ibkr_autologin_watch.py
```


## [2026-02-18 19:55:49] FEATURE
Neue Funktionalität hinzugefügt - (+1438/-0 Zeilen)

**Statistics:**
- Files Changed: 1
- Lines Added: +1438
- Lines Deleted: -0
- Net Change: +1438

**File-Level Changes:**
```
+ 1438 -    0  scripts/octa_ibkr_autologin_watch.py
```


## [2026-02-18 19:35:07] FEATURE
Neue Funktionalität hinzugefügt - 1 Dokumentations-Dateien - (+745/-17 Zeilen)

**Statistics:**
- Files Changed: 3
- Lines Added: +745
- Lines Deleted: -17
- Net Change: +728

**File-Level Changes:**
```
+  584 -    5  docs/IBKR_X11_AUTOLOGIN.md
+  150 -    0  scripts/octa_ibkr_autologin_healthcheck.sh
+   11 -   12  systemd/octa-autologin.service
```


## [2026-02-18 18:15:35] FEATURE
Neue Funktionalität hinzugefügt - 19 Code-Dateien - 9 Test-Dateien - 1 Dokumentations-Dateien - (+3753/-16 Zeilen)

**Statistics:**
- Files Changed: 30
- Lines Added: +3753
- Lines Deleted: -16
- Net Change: +3737

**File-Level Changes:**
```
+    8 -    0  .gitignore
+  169 -    0  docs/GOVERNANCE_POLICY.md
+    1 -    0  octa/accounting/__init__.py
+  225 -    0  octa/accounting/capital_ledger.py
+    0 -    0  octa/accounting/ops/__init__.py
+   44 -    0  octa/accounting/ops/reconcile.py
+  204 -    0  octa/core/data/quality/sanitizer.py
+   10 -    0  octa/core/governance/__init__.py
+  172 -    0  octa/core/governance/artifact_signing.py
+  127 -    0  octa/core/governance/derivatives_gate.py
+  161 -    0  octa/core/governance/emir.py
+  137 -    0  octa/core/governance/governance_audit.py
+  174 -    0  octa/core/governance/keystore.py
+  122 -    0  octa/core/governance/lei_registry.py
+  196 -    0  octa/core/orchestration/training_fingerprint.py
+  187 -    0  octa/core/portfolio/preflight.py
+  113 -   16  octa/execution/runner.py
+    0 -    0  octa/models/__init__.py
+  146 -    0  octa/models/approved_loader.py
+    0 -    0  octa/models/ops/__init__.py
... and 10 more files
```


## [2026-02-17 18:05:36] FEATURE
Neue Funktionalität hinzugefügt - 1 Code-Dateien - 2 Test-Dateien - 1 Dokumentations-Dateien - (+1360/-22 Zeilen)

**Statistics:**
- Files Changed: 6
- Lines Added: +1360
- Lines Deleted: -22
- Net Change: +1338

**File-Level Changes:**
```
+  184 -    0  docs/IBKR_X11_AUTOLOGIN.md
+  585 -    0  octa/execution/ibkr_x11_login.py
+   79 -    4  scripts/octa_autologin_bootstrap.sh
+  138 -   18  scripts/octa_ibkr_bootstrap.sh
+   52 -    0  tests/test_bootstrap_no_bare_python.py
+  322 -    0  tests/test_ibkr_x11_login_steps.py
```


## [2026-02-17 15:27:31] DOCS
Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - 1 Dokumentations-Dateien - (+111/-48 Zeilen)

**Statistics:**
- Files Changed: 5
- Lines Added: +111
- Lines Deleted: -48
- Net Change: +63

**File-Level Changes:**
```
+   14 -    0  docs/SYSTEMD_AUTOSTART.md
+    9 -    1  octa/support/ops/v000_full_universe_cascade_train.py
+   73 -   42  scripts/octa_x11_bootstrap.sh
+    4 -    3  systemd/octa-x11.service
+   11 -    2  tests/test_systemd_smoke.py
```


## [2026-02-17 15:17:38] DOCS
Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - 1 Dokumentations-Dateien - (+186/-36 Zeilen)

**Statistics:**
- Files Changed: 12
- Lines Added: +186
- Lines Deleted: -36
- Net Change: +150

**File-Level Changes:**
```
+    6 -    0  docs/SYSTEMD_AUTOSTART.md
+   36 -   11  octa/support/ops/v000_full_universe_cascade_train.py
+    9 -    2  scripts/octa_autologin_bootstrap.sh
+   15 -    1  scripts/octa_health_watchdog.py
+   35 -    1  scripts/octa_ibkr_bootstrap.sh
+    2 -    1  scripts/octa_systemd_entrypoint.sh
+   60 -   16  scripts/octa_x11_bootstrap.sh
+    2 -    0  systemd/octa-autologin.service
+    2 -    0  systemd/octa-ibkr.service
+    2 -    0  systemd/octa-v000.service
+    7 -    4  systemd/octa-x11.service
+   10 -    0  tests/test_systemd_smoke.py
```


## [2026-02-17 14:48:56] FEATURE
Neue Funktionalität hinzugefügt - 6 Code-Dateien - 5 Test-Dateien - 1 Dokumentations-Dateien - (+2042/-12 Zeilen)

**Statistics:**
- Files Changed: 24
- Lines Added: +2042
- Lines Deleted: -12
- Net Change: +2030

**File-Level Changes:**
```
+   46 -    0  docs/SYSTEMD_AUTOSTART.md
+  194 -    0  octa/execution/ibkr_autologin_store.py
+  139 -    0  octa/execution/ibkr_runtime.py
+  330 -    0  octa/execution/ibkr_x11_autologin.py
+  315 -    0  octa/support/ops/v000_full_universe_cascade_train.py
+  287 -   10  octa_ops/autopilot/cascade_train.py
+  100 -    2  octa_training/core/pipeline.py
+   12 -    0  scripts/octa_autologin_bootstrap.sh
+   65 -    0  scripts/octa_health_watchdog.py
+   42 -    0  scripts/octa_ibkr_bootstrap.sh
+   35 -    0  scripts/octa_ibkr_teach.py
+   18 -    0  scripts/octa_systemd_entrypoint.sh
+   36 -    0  scripts/octa_v000_loop.sh
+   34 -    0  scripts/octa_x11_bootstrap.sh
+   18 -    0  systemd/octa-autologin.service
+   18 -    0  systemd/octa-ibkr.service
+   18 -    0  systemd/octa-v000.service
+   19 -    0  systemd/octa-x11.service
+    4 -    0  systemd/octa.target
+   97 -    0  tests/test_cascade_structural_vs_performance.py
... and 4 more files
```


## [2026-02-15 14:40:09] FEATURE
Neue Funktionalität hinzugefügt - 1 Code-Dateien - 1 Test-Dateien - (+519/-0 Zeilen)

**Statistics:**
- Files Changed: 2
- Lines Added: +519
- Lines Deleted: -0
- Net Change: +519

**File-Level Changes:**
```
+  447 -    0  octa/support/ops/v000_finish_paper_ready_local_only.py
+   72 -    0  tests/test_v000_finish_paper_ready_local_only_synth.py
```


## [2026-02-13 16:47:38] FEATURE
Neue Funktionalität hinzugefügt - (+3412/-0 Zeilen)

**Statistics:**
- Files Changed: 64
- Lines Added: +3412
- Lines Deleted: -0
- Net Change: +3412

**File-Level Changes:**
```
+   28 -    0  data/altdat/meta/features_AVAH_1D_altdat_20260213T153616Z_AVAH_1D.json
+   28 -    0  data/altdat/meta/features_AVAH_1D_altdat_20260213T153618Z_AVAH_1D.json
+   28 -    0  data/altdat/meta/features_AVAH_1D_altdat_20260213T153619Z_AVAH_1D.json
+   28 -    0  data/altdat/meta/features_AVAH_1D_altdat_20260213T153620Z_AVAH_1D.json
+   28 -    0  data/altdat/meta/features_AVAL_1D_altdat_20260213T153646Z_AVAL_1D.json
+   28 -    0  data/altdat/meta/features_AVAL_1D_altdat_20260213T153647Z_AVAL_1D.json
+   28 -    0  data/altdat/meta/features_AVAL_1D_altdat_20260213T153650Z_AVAL_1D.json
+   28 -    0  data/altdat/meta/features_AVAL_1D_altdat_20260213T153653Z_AVAL_1D.json
+   28 -    0  data/altdat/meta/features_AVAV_1D_altdat_20260213T153816Z_AVAV_1D.json
+   28 -    0  data/altdat/meta/features_AVAV_1D_altdat_20260213T153817Z_AVAV_1D.json
+   28 -    0  data/altdat/meta/features_AVAV_1D_altdat_20260213T153818Z_AVAV_1D.json
+   28 -    0  data/altdat/meta/features_AVAV_1D_altdat_20260213T153819Z_AVAV_1D.json
+   28 -    0  data/altdat/meta/features_AVBC_1D_altdat_20260213T154212Z_AVBC_1D.json
+   28 -    0  data/altdat/meta/features_AVBC_1D_altdat_20260213T154213Z_AVBC_1D.json
+   28 -    0  data/altdat/meta/features_AVBH_1D_altdat_20260213T154214Z_AVBH_1D.json
+   28 -    0  data/altdat/meta/features_AVBH_1D_altdat_20260213T154215Z_AVBH_1D.json
+   28 -    0  data/altdat/meta/features_AVBP_1D_altdat_20260213T154231Z_AVBP_1D.json
+   28 -    0  data/altdat/meta/features_AVBP_1D_altdat_20260213T154232Z_AVBP_1D.json
+   28 -    0  data/altdat/meta/features_AVBP_1D_altdat_20260213T154233Z_AVBP_1D.json
+   28 -    0  data/altdat/meta/features_AVBP_1D_altdat_20260213T154234Z_AVBP_1D.json
... and 44 more files
```


## [2026-02-13 16:40:51] BUGFIX
Fehlerbehebung - 25 Code-Dateien - 13 Test-Dateien - 2 Dokumentations-Dateien - (+408429/-481537961 Zeilen)

**Statistics:**
- Files Changed: 9657
- Lines Added: +408429
- Lines Deleted: -481537961
- Net Change: -481129532

**File-Level Changes:**
```
+   31 -    0  ARCHITECTURE_v0_0_0.md
+   35 -    0  README_v0_0_0.md
+   28 -    0  data/altdat/meta/features_AACBR_1D_altdat_20260211T211304Z_AACBR_1D.json
+   28 -    0  data/altdat/meta/features_AACBR_1D_altdat_20260211T211305Z_AACBR_1D.json
+   28 -    0  data/altdat/meta/features_AACBR_1D_altdat_20260211T211413Z_AACBR_1D.json
+   28 -    0  data/altdat/meta/features_AACBR_1D_altdat_20260211T211414Z_AACBR_1D.json
+   28 -    0  data/altdat/meta/features_AACBR_1D_altdat_20260211T211439Z_AACBR_1D.json
+   28 -    0  data/altdat/meta/features_AACBR_1D_altdat_20260212T054525Z_AACBR_1D.json
+   28 -    0  data/altdat/meta/features_AACBR_1D_altdat_20260212T054526Z_AACBR_1D.json
+   28 -    0  data/altdat/meta/features_AACBR_1D_altdat_20260212T063101Z_AACBR_1D.json
+   28 -    0  data/altdat/meta/features_AACBR_1D_altdat_20260212T063102Z_AACBR_1D.json
+   28 -    0  data/altdat/meta/features_AACBR_1D_altdat_20260212T063103Z_AACBR_1D.json
+   28 -    0  data/altdat/meta/features_AACBR_1D_altdat_20260213T052257Z_AACBR_1D.json
+   28 -    0  data/altdat/meta/features_AACBR_1D_altdat_20260213T052258Z_AACBR_1D.json
+   28 -    0  data/altdat/meta/features_AACBU_1D_altdat_20260211T211305Z_AACBU_1D.json
+   28 -    0  data/altdat/meta/features_AACBU_1D_altdat_20260211T211306Z_AACBU_1D.json
+   28 -    0  data/altdat/meta/features_AACBU_1D_altdat_20260211T211415Z_AACBU_1D.json
+   28 -    0  data/altdat/meta/features_AACBU_1D_altdat_20260211T211416Z_AACBU_1D.json
+   28 -    0  data/altdat/meta/features_AACBU_1D_altdat_20260211T211440Z_AACBU_1D.json
+   28 -    0  data/altdat/meta/features_AACBU_1D_altdat_20260212T054526Z_AACBU_1D.json
... and 9637 more files
```


## [2026-02-13 13:23:07] CHANGE
Code-Änderungen - 1 Code-Dateien - (+5/-1 Zeilen)

**Statistics:**
- Files Changed: 1
- Lines Added: +5
- Lines Deleted: -1
- Net Change: +4

**File-Level Changes:**
```
+    5 -    1  octa/core/features/features.py
```


## [2026-02-13 06:16:05] CHANGE
Code-Änderungen - 5 Code-Dateien - (+270/-39 Zeilen)

**Statistics:**
- Files Changed: 6
- Lines Added: +270
- Lines Deleted: -39
- Net Change: +231

**File-Level Changes:**
```
+   28 -    1  octa/core/data/sources/altdata/cache.py
+  164 -   19  octa/core/features/features.py
+    2 -    2  octa/core/features/transforms/feature_builder.py
+    8 -    5  octa_training/config/training.yaml
+   56 -   10  octa_training/core/pipeline.py
+   12 -    2  octa_training/run_train.py
```


## [2026-02-11 22:01:28] TEST
Code-Änderungen - 1 Code-Dateien - 1 Test-Dateien - (+413/-170 Zeilen)

**Statistics:**
- Files Changed: 2
- Lines Added: +413
- Lines Deleted: -170
- Net Change: +243

**File-Level Changes:**
```
+  342 -  134  octa/support/ops/build_raw_tree_from_flat_parquets.py
+   71 -   36  tests/test_build_raw_tree_from_flat_parquets.py
```


## [2026-02-11 21:55:33] CONFIG
Code-Änderungen - 3 Code-Dateien - 7 Test-Dateien - (+232/-49 Zeilen)

**Statistics:**
- Files Changed: 11
- Lines Added: +232
- Lines Deleted: -49
- Net Change: +183

**File-Level Changes:**
```
+    2 -    1  config/pytest.ini
+    4 -    0  octa/core/system_tests/test_full_autonomy_parquet_e2e.py
+    4 -    0  octa/core/system_tests/test_full_autonomy_parquet_e2e_5symbols.py
+   41 -    0  octa/support/ops/run_evidence_smoke.sh
+   24 -   18  octa/support/ops/run_full_cascade_training_from_parquets.py
+   29 -    4  octa_ops/autopilot/cascade_train.py
+   42 -   25  octa_training/core/pipeline.py
+    1 -    0  tests/integration/test_feast_materialize.py
+   48 -    0  tests/test_autopilot_cascade_pkl_per_timeframe.py
+    2 -    0  tests/test_control_plane_snapshots.py
+   35 -    1  tests/test_run_full_cascade_training_from_parquets.py
```


## [2026-02-11 21:26:29] TEST
Code-Änderungen - 2 Code-Dateien - 2 Test-Dateien - (+420/-2 Zeilen)

**Statistics:**
- Files Changed: 4
- Lines Added: +420
- Lines Deleted: -2
- Net Change: +418

**File-Level Changes:**
```
+   13 -    2  octa/core/data/io/io_parquet.py
+  304 -    0  octa/support/ops/build_raw_tree_from_flat_parquets.py
+   74 -    0  tests/test_build_raw_tree_from_flat_parquets.py
+   29 -    0  tests/test_io_parquet_recursive_discovery.py
```


## [2026-02-11 21:17:47] FEATURE
Neue Funktionalität hinzugefügt - 6 Code-Dateien - 2 Test-Dateien - (+1077/-37 Zeilen)

**Statistics:**
- Files Changed: 8
- Lines Added: +1077
- Lines Deleted: -37
- Net Change: +1040

**File-Level Changes:**
```
+   78 -    1  octa/support/ops/run_full_cascade_training_from_parquets.py
+   22 -    1  octa_ops/autopilot/cascade_train.py
+   23 -    0  octa_training/core/gates.py
+  532 -    0  octa_training/core/institutional_gates.py
+   24 -    2  octa_training/core/pipeline.py
+  118 -   26  octa_training/core/robustness.py
+  155 -    0  tests/test_institutional_gates_mandatory.py
+  125 -    7  tests/test_run_full_cascade_training_from_parquets.py
```


## [2026-02-11 20:49:27] TEST
Code-Änderungen - 9 Code-Dateien - 3 Test-Dateien - (+2268/-108 Zeilen)

**Statistics:**
- Files Changed: 13
- Lines Added: +2268
- Lines Deleted: -108
- Net Change: +2160

**File-Level Changes:**
```
+    7 -    1  octa/core/data/sources/altdata/orchestrator.py
+   67 -    2  octa/core/features/features.py
+  159 -   67  octa/core/features/transforms/feature_builder.py
+ 1007 -    0  octa/support/ops/run_full_cascade_training_from_parquets.py
+   70 -    6  octa_ops/autopilot/cascade_train.py
+   42 -    5  octa_training/config/training.yaml
+    8 -    0  octa_training/core/gates.py
+   31 -    1  octa_training/core/packaging.py
+  123 -    5  octa_training/core/pipeline.py
+  173 -   21  octa_training/core/robustness.py
+   88 -    0  tests/test_altdata_fail_soft_deterministic.py
+   55 -    0  tests/test_monte_carlo_mandatory.py
+  438 -    0  tests/test_run_full_cascade_training_from_parquets.py
```


## [2026-02-11 20:11:48] CHANGE
Code-Änderungen - (+4/-0 Zeilen)

**Statistics:**
- Files Changed: 2
- Lines Added: +4
- Lines Deleted: -0
- Net Change: +4

**File-Level Changes:**
```
+    4 -    0  .gitignore
+    0 -    0  state/state.db
```

