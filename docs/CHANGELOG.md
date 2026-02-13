# Octa Changelog

Automatisch generiert. Alle Änderungen werden hier dokumentiert.

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

