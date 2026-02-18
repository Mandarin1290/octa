# Technical Changelog

Detaillierte technische Änderungen. Auto-generiert.


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

