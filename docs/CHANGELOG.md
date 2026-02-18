# Octa Changelog

Automatisch generiert. Alle Änderungen werden hier dokumentiert.

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

