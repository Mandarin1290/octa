# OCTA Library Map — Empfehlung & Aktionsplan

Ziel: Für jede Capability den aktuellen Zustand, empfohlene Bibliothek und konkrete Aktion (minimal invasive Änderungen) dokumentieren.

- **Backtesting / Replay**
  - Current: Keine Zipline- oder Backtrader-Integration gefunden; ad-hoc replay code (scattered). (Found: no `zipline` imports)
  - Recommended: `zipline` (institutional research/backtest) oder `backtrader`/`vectorbt` als Backup
  - Action: Implement Zipline connector for parquet bundles; add migration notes. (See OCTA_FIX_PACK/0002_zipline_connector.patch)

- **Portfolio Analytics / Performance**
  - Current: Custom analytics; no standardized `empyrical`/`pyfolio`/`quantstats` usage detected.
  - Recommended: `empyrical` + `pyfolio` or `quantstats` for reporting and analytics
  - Action: Replace hand-rolled metrics with `empyrical` wrappers; add tests.

- **Risk / Optimization**
  - Current: Heuristic allocators and drawdown playbooks (`octa_sentinel/drawdown_playbook.py`).
  - Recommended: `scipy`/`statsmodels`/`arch` for stats; `cvxpy` or `PyPortfolioOpt` for optimizations.
  - Action: Replace ad-hoc optimizer with `cvxpy` backed routines where convergent solutions needed.

- **Data Storage & Querying**
  - Current: Parquet/pyarrow used informally; no canonical ingestion connector.
  - Recommended: `pandas` + `pyarrow` + `duckdb` for fast ingestion; use `feather`/`parquet` canonical formats
  - Action: Create standardized ingestion util `octa_core/io/parquet_bundle.py` and Zipline-bundle converter.

- **ML / Models**
  - Current: `octa_ml` has refresh machinery, but model artifact format not enforced.
  - Recommended: `scikit-learn` Pipelines for preprocessing + `joblib` or `onnx` for artifact serialization; `lightgbm`/`xgboost` where needed
  - Action: Add `ModelIO` wrapper enforcing joblib + metadata and lineage registration.

- **Orchestration**
  - Current: Runbooks + ops scripts exist; no enterprise scheduler clearly required.
  - Recommended: `prefect` for workflows (optional) or maintain robust internal scheduler with strict tests.
  - Action: If adopting Prefect, add connector; otherwise add strong unit tests for internal scheduler.

- **Config & Validation**
  - Current: `octa_ip/config_isolation.py` enforces policies.
  - Recommended: `pydantic` or `hydra` for strict config validation.
  - Action: Add `pydantic` schemas where config dictionaries are used.

- **Logging & Audit**
  - Current: Audit engine present (`octa_audit`) with hash-chain mentions.
  - Recommended: Structured logging (`structlog`/`python-json-logger`) + cryptographic hash chains for immutability.
  - Action: Add automated audit verification tests and a signed evidence collector.

---

Anmerkung: Alle vorgeschlagenen Library-Replacements sollen nur dort angewandt werden, wo ein klare technische Überlegenheit und reduzierte Fehlergefahr nachweisbar ist. Patches mit minimaler Blast-Radius werden in `OCTA_FIX_PACK/` bereitgestellt.
 
### Connector-Integrationen (neu)

- `octa_core/backtest/vectorbt_connector.py`: Adapter für `vectorbt`. Importiert `vectorbt` nur zur Laufzeit
  und liefert ein kleines `stats`-Objekt plus das `Portfolio`-Objekt für tiefere Inspektionen.

- `octa_core/backtest/zipline_connector.py`: Shim/Anlaufstelle für `zipline`-basierte Backtests.
  Liefert klare Fehlermeldungen, wenn `zipline` nicht installiert ist, und dokumentiert die
  erwarteten Integrationsschritte für eine Produktionsanbindung.

Integrationsempfehlungen:
- Vereinheitlichen Sie die Eingangsformate (`price_df`, `entries`, `exits`) als pandas-Objekte
  mit DatetimeIndex bevor Sie die Connectoren aufrufen.
- Verwenden Sie die Connectoren als einzigen Integrationspunkt, damit ein Wechsel der Engine
  nur an einer Stelle vorgenommen werden muss.
