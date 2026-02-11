# Technical Changelog

Detaillierte technische Änderungen. Auto-generiert.


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

