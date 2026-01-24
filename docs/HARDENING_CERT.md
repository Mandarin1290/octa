**Hardening Certification (Evidence‑Only)**

Kurzbeschreibung
- `octa_reports.hardening_cert.HardeningCertification` generiert deterministische, auditable Zertifikate basierend ausschließlich auf maschinenlesbarer Evidenz.
- Keine subjektiven Aussagen: der Report enthält nur Zähl‑ und Faktendaten (Testergebnisse, Drill‑Ergebnisse, numerische Scores, gelistete, ungelöste Risiken) und einen kanonischen Report‑Hash.

Erforderliche Eingaben (Beispiele)
- `chaos_tests`: Liste von Records {"name": str, "passed": bool, "evidence": {...}}.
- `kill_switch_drills`: Liste von Records {"name": str, "passed": bool, "evidence": {...}}.
- `resilience_scores`: Liste von Records {"component": str, "score": float}.
- `unresolved_risks`: Liste von Records {"id": str, "description": str}.
- Optional: `evidence_store`: Mapping evidence_id -> payload (will be hashed deterministically).

Ausgabe
- `evidence`: die normalisierte Evidenz (only allowed keys)
- `summary`: faktische Metriken (counts and pass counts)
- `created_at`: ISO8601 UTC timestamp
- `report_hash`: SHA256 over canonical JSON of report (deterministic)

Beispiel
```py
from octa_reports.hardening_cert import HardeningCertification

evidence = {
    "chaos_tests": [{"name": "failure_injector_test", "passed": True, "evidence": {"trace": "..."}}],
    "kill_switch_drills": [{"name": "kill_switch_flatten", "passed": True, "evidence": {"audit": "..."}}],
    "resilience_scores": [{"component": "strategies", "score": 0.87}],
    "unresolved_risks": [{"id": "R1", "description": "external dependency lag"}],
}

cert = HardeningCertification(evidence)
report = cert.generate()
print(cert.export_json())
```

Hinweise
- Bewahre `report_hash` und den zugehörigen `export_json()` Output in einem WORM/Audit-Store.
- Der Report ist dafür gedacht, maschinen‑verifizierbare Aussagen zu treffen; Ergänzende narrative Zusammenfassungen gehören in getrennte, menschlich geprüfte Dokumente.
