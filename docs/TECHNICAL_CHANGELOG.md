# Technical Changelog

Detaillierte technische Änderungen. Auto-generiert.


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

