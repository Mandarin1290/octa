**Governance Escalation Paths**

Kurzbeschreibung
- `octa_governance.escalation.EscalationManager` verwaltet Escalations, prüft Approvals und protokolliert alle Schritte.
- Keine einzelne:r Akteur:in hat absolute Kontrolle — Auflösung erfordert mindestens zwei unabhängige Approvals (konfigurierbar) und optional spezifische Rollen.

Escalation Types
- `risk_vs_execution` — Konflikte zwischen Risikomanagement und Ausführung
- `audit_anomaly` — Ungewöhnliche Audit‑Signals
- `external_incident` — Externe Störung oder Meldung

Wesentliche Regeln
- Jede Escalation wird mit `trigger_escalation()` erzeugt und erhält eine UUID.
- Approvals werden mit `add_approval()` hinzugefügt; doppelte Approvals desselben Actors werden ignoriert.
- `resolve_escalation()` verlangt mindestens `required_approval_count` unterschiedliche Approvals; falls `required_roles` angegeben sind, müssen diese Rollen ebenfalls genehmigt haben.
- Alle Aktionen werden append‑only in `EscalationManager.audit_log` geschrieben und enthalten `ts`, `actor`, `action`, `details`.

Beispiel
```py
from octa_governance.escalation import EscalationManager

mgr = EscalationManager()
esc = mgr.trigger_escalation("risk_vs_execution", {"conflict": "limit_override"}, created_by="alice", required_approval_count=2, required_roles={"risk","ops"})
mgr.add_approval(esc.id, "bob", "risk")
mgr.add_approval(esc.id, "carol", "ops")
mgr.resolve_escalation(esc.id, resolved_by="diana")
```

Sicherheits‑Hinweis
- Wähle `required_approval_count` und `required_roles` so, dass kein einzelner Ausfall oder böswilliger Akteur das System kontrollieren kann.
