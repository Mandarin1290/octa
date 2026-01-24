**Regulatory Evolution Adaptation Layer**

Zweck
- Ermöglicht, dass regulatorische Änderungen systematisch und auditierbar verwaltet werden, ohne das System komplett neu zu designen.

Hauptmerkmale
- Rule Abstraction: Regelobjekte (`Rule`) mit `rule_id`, `version`, `jurisdiction`, `effective_date`, `content` und `metadata`.
- Jurisdiction Tagging: Jede Regel trägt das betroffene Rechtsgebiet (`jurisdiction`).
- Compliance Evolution Log: Append‑only, hash‑verketteter Log der Änderungen mit `verify_evolution_log()`.
- Versionierung & Kompatibilität: Neue Versionen müssen Kompatibilitätsprüfungen bestehen (z. B. `required_fields`).

API Kurzüberblick
- `RegulatoryAdaptation()` — Instanz.
- `add_rule(user, rule)` — fügt neue Regel (root) hinzu.
- `add_rule_version(user, rule_id, new_rule, compatibility_mode='strict')` — fügt neue Version hinzu (Prüfung gegen parent).
- `get_rule(rule_id, version)` — liest Regelversion.
- `evolution_log()` — exportiert Log.
- `verify_evolution_log()` — prüft Integrität der Log‑Kette.

Designprinzipien
- System passt sich an, indem es Regeln als Daten verwaltet; operative Logik bleibt getrennt.
- Änderungen werden auditiert; jede Version hat deterministischen `evidence_hash`.
- Backward‑compatibility verhindert ungeplante Brüche in laufenden Systemen.

Beispiel

```
from octa_compliance.regulatory_adapt import RegulatoryAdaptation, Rule
ra = RegulatoryAdaptation()
rule_v1 = Rule(rule_id='KYC', version='1.0', jurisdiction='DE', effective_date='2026-01-01', content={'fields':['name','id']}, metadata={'required_fields':['name','id']})
ra.add_rule(user='compliance_officer', rule=rule_v1)

rule_v2 = Rule(rule_id='KYC', version='1.1', jurisdiction='DE', effective_date='2026-06-01', content={'fields':['name','id','email']}, metadata={'required_fields':['name','id']}, parent='1.0')
ra.add_rule_version(user='compliance_officer', rule_id='KYC', new_rule=rule_v2)

print(ra.verify_evolution_log())
```

Tests
- `tests/test_regulatory_adapt.py` prüft Rule‑Versioning und Kompatibilitätsregeln.
