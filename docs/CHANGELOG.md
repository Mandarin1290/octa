# Octa Changelog

Automatisch generiert. Alle Änderungen werden hier dokumentiert.

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

