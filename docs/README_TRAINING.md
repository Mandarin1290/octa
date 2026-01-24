OCTa Training - Betriebsanleitung
================================

Kurz: Diese Datei beschreibt Setup, Ausführung, Konfiguration, Artefakt-Lifecycle und Troubleshooting für das Octa-Training-Pipeline-System.

Setup
-----
- Python-Umgebung: Empfohlen Python 3.10+. Installiere Abhängigkeiten aus `requirements.txt`.
- Optional: `lightgbm`, `xgboost`, `catboost` für Boosting-Modelle. APScheduler für Daemon.

Wichtige Pfade
- Rohdaten: `raw/` (Parquet-Dateien)
- Artifacts (PKL/meta): `raw/PKL/`
- Quarantine: `raw/PKL/_quarantine/<symbol>/<timestamp>/`
- State DB: `state/state.db`
- Reports: `reports/daemon/YYYYMMDD/`

Commands
--------
- Einmal-Training (CLI):
  - `python -m octa_training.run_train --symbol <SYM> --package --evaluate --smoke-test-after-package`
- Daemon (periodisch):
  - `python -m octa_training.run_daemon` (falls `apscheduler` installiert)
- Artefakt-Inspektion:
  - `python -m octa_training.tools.inspect_artifact --path raw/PKL/<symbol>.pkl`

Konfigurationsübersicht
-----------------------
- `paths`: Rohdaten, PKL, logs, state, reports
- `retrain.skip_window_days`: Wenn letztes PASS innerhalb dieser Tage liegt, wird das Training übersprungen (Idempotenz)
- `packaging.compare_metric_name`: Metrik (z.B. `sharpe`) zur Evaluierung, `min_improvement` minimaler Abstand
- `packaging.max_age_days`: Erlaube Überschreiben, wenn bestehendes Artefakt älter als dieser Wert
- `packaging.quarantine_on_smoke_fail`: Verschiebe fehlerhafte PKLs in Quarantäne
- `robustness`-Defaults: Permutation-, Subwindow-, Stress- und Regime-Thresholds

Asset Profiles (Global Gate)
----------------------------
Die Global-Gate-Schwellenwerte können optional per Asset-Profil geroutet und überschrieben werden.

- Routing-Konfig: `asset_defaults` (per `dataset`/`asset_class`)
- Profile-Definition: `asset_profiles.<name>.gates` (gleiche Key-Namen wie `GateSpec`/`cfg.gates`)
- Fail-closed/Backwards-kompatibel: Wenn keine Profile konfiguriert sind, wird automatisch das Profil `legacy` genutzt.

Ein minimales Beispiel findest du in `configs/asset_profiles_example.yaml`.

Artifact Lifecycle
------------------
1. Training → Evaluation → Robustness-Checks
2. Packaging: erzeugt `<symbol>.pkl`, `<symbol>.sha256`, `<symbol>.meta.json` (inkl. `schema_version`)
3. Smoke-Test: Nach Packaging wird ein Smoke-Test ausgeführt. Bei Fehlschlag wird das Artefakt nach `raw/PKL/_quarantine/<symbol>/<timestamp>/` verschoben (inkl. `quarantine_reason.txt`) und der State aktualisiert.
4. Replacement-Policy: Neues Artefakt ersetzt ein bestehendes nur, wenn es die Vergleichsmetrik verbessert (oder das existierende Artefakt älter als `packaging.max_age_days`).

Backward Compatibility
----------------------
- Metadatei enthält `schema_version`. Der Loader versucht ältere Versionen best-effort zu lesen. Artefakte mit deutlich neuerer `schema_version` werden nicht geladen und sollten manuell geprüft oder quarantined werden.

Troubleshooting
---------------
- `ModuleNotFoundError: apscheduler`: optional, der Daemon benötigt `apscheduler`. Ohne diese Bibliothek läuft die CLI weiterhin, der Daemon nutzt dann kein Scheduler-Feature.
- SHA-Mismatch bei Ladeversuch: Artefakt gilt als korrupt; schaue in `raw/PKL/_quarantine/<symbol>/` nach verschobenen Dateien oder starte Re-Train.
- GPU-Fallback: Wenn GPU-Parameter fehlschlagen, fällt das Training auf CPU zurück (Model-Code behandelt Geräte-spezifische Parameter sicher).
- NaNs/fehlende Features: Smoke-Test oder Feature-Build kann fehlschlagen falls Features fehlen; prüfe Parquet-Integrität und Feature-Config.

Betriebs-Checks
---------------
- Prüfe `state/state.db` (`sqlite3 state/state.db`) um Queue/Status zu sehen.
- Reports werden unter `reports/daemon/` abgespeichert pro Run.

Kontakt
-------
Für tiefergehende Probleme: Repo-Maintainer oder Dev-Ops Team.
