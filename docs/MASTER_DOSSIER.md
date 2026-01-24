**Institutional Readiness — Master Dossier**

Zweck
- Erzeugt ein evidenzbasiertes Dossier, das Architektur, Governance‑ und Risiko‑Nachweise, Audit‑Readiness, Longevity‑Zertifikate und eine IP‑Registry‑Zusammenfassung enthält.
- Kein Marketing; nur überprüfbare Fakten und deterministische Evidence‑Hashes.

Inhalte
- Architekturübersicht: Dateizählung, LOC, Stichprobenliste, Architektur‑Evidence‑Hash.
- IP‑Registry‑Zusammenfassung: Knotenzahl und Evidence‑Hash der Registry.
- Audit‑Readiness: Anzahl Audit‑Einträge, Verifizierungsflag, neuester Log‑Eintrag.
- Governance‑Proof: Audit‑Trail‑Länge, veröffentlichtes Manifest‑Hash (sofern vorhanden).
- Longevity‑Zertifikat: aggregierte Zertifikats‑Daten und Evidence‑Hash.

Erzeugung
- Verwende `octa_reports.master_dossier.MasterDossier(repo_path=..., subsystems={...})`.
- Aufruf `generate()` liefert ein serialisierbares Dossier‑Objekt mit deterministischem `dossier_hash`.

Beispiel

```
from octa_reports.master_dossier import MasterDossier
from octa_ip.ip_registry import IPRegistry
from octa_audit.audit_interface import AuditInterface

reg = IPRegistry()
# ... populate registry ...
audit = AuditInterface(registry=reg)

md = MasterDossier(repo_path='.', subsystems={'ip_registry': reg, 'audit': audit})
dossier = md.generate()
print(dossier['dossier_hash'])
```

Hinweise
- Dossier ist Evidence‑only: die Felder enthalten Zählungen, Zeitstempel, deterministische Hashes und rohe manifest‑Daten; keine subjektiven Bewertungen oder Marketingformulierungen.
- Zur externen Auditierung empfiehlt sich Signieren des erzeugten JSON‑Manifests und Offsite‑Archivierung.
