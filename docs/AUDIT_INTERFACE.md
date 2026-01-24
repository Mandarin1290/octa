**External Audit Interface (Read-Only)**

Kurzbeschreibung:
- Stellt einen kontrollierten, schreibgeschützten Zugang zu Beweisdaten (Registry, Governance, NAV, Model Lineage) bereit.
- Jeder Zugriff wird protokolliert in einem append-only Hash‑Kette (tamper‑evident).
- Die Schnittstelle liefert nur evidence (Hashes, timestamps, metadata) und niemals Ausführungs- oder Handle‑Objekte.

API Übersicht:
- `AuditInterface(registry=None, governance=None, accounting=None, models=None)` — Konstruktor.
- `access_registry_snapshot(user, query=None) -> dict` — sichere Momentaufnahme von `asset@version -> {evidence_hash,lifecycle,created_at}`.
- `access_governance_log(user, query=None) -> dict` — liest Governance‑Audittrail (falls verfügbar) als serialisierbare Struktur.
- `access_accounting_nav(user, query=None) -> dict` — liest NAV/Capital‑Trails (falls verfügbar) als serialisierbare Struktur.
- `access_model_lineage(user, query=None) -> dict` — liest modellbezogene Lineage‑Summaries (name, version, evidence_hash).
- `list_logs() -> List[Dict]` — defensiver Export der Audit‑Logs.
- `verify_logs() -> bool` — prüft Integrität der Log‑Kette.

Sicherheitsprinzipien:
- Read‑only: Rückgaben sind Deep‑copies / serialisierte Objekte; keine Funktionen oder Live‑Referenzen werden zurückgegeben.
- Minimal‑Exposure: Nur Evidence‑Felder werden bereitgestellt (z. B. `evidence_hash`).
- Tamper‑evidence: Jede Log‑Einheit enthält `prev_hash` und `entry_hash`; `verify_logs()` prüft Konsistenz.

Beispielgebrauch:

```
from octa_audit.audit_interface import AuditInterface
from octa_ip.ip_registry import IPRegistry

reg = IPRegistry()
reg.add_asset("alpha", asset_id="alpha")
reg.add_version("alpha", "v1")

audit = AuditInterface(registry=reg)
snap = audit.access_registry_snapshot(user="auditor@example.com")
print(snap)
print(audit.verify_logs())
```

Hinweis zur Persistenz und externem Audit:
- Für externe Audits empfiehlt sich, `audit.list_logs()` regelmäßig als signed JSON‑Manifest zu exportieren und offline zu archivieren.
