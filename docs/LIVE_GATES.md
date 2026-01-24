**Live‑Readiness Gates**

Kurzbeschreibung
- `octa_core.live_gates.LiveGates` fasst vier kritische Gates zusammen: `risk_metrics`, `execution_health`, `data_integrity` und `governance_clearance`.
- Alle Gates müssen erfolgreich sein, damit Live‑Trading freigegeben wird. Bei einem Fehlschlag wird Kapital blockiert.

Gates
- risk_metrics: Vergleicht Kennzahlen (z.B. drawdown, VaR) mit Schwellwerten.
- execution_health: Prüft Latenz, Verbindungsstatus und Fehlerquoten.
- data_integrity: Prüft Aktualität von Datenquellen (max_age_s).
- governance_clearance: Prüft erforderliche Genehmigungen (Anzahl und Rollen).

Audit
- Alle Bewertungen und Entscheidungen werden append‑only in `LiveGates.audit_log` abgelegt mit `ts`, `actor`, `action`, `details`.

Beispiel
```py
from octa_core.live_gates import LiveGates

lg = LiveGates()
risk_metrics = {"max_drawdown": 0.05}
thresholds = {"max_drawdown": 0.1}
execution = {"connected": True, "latency_ms": 120, "latency_threshold_ms": 500, "failure_rate": 0.0, "failure_rate_threshold": 0.01}
data_checks = {"price_feed": {"last_update_age_s": 2, "max_age_s": 10}}
governance = {"approvals": ["alice","bob"], "approved_roles": ["risk","ops"], "required_count": 2, "required_roles": ["risk","ops"]}

lg.enforce_live(risk_metrics, thresholds, execution, data_checks, governance, actor="ops_user")
```

Hinweis
- Konfiguriere die Schwellwerte und Governance‑Regeln konservativ — bei Live‑Trading ist Blockieren bei Unsicherheit die sichere Option.
