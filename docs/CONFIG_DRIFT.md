**Config Drift Detection**

Kurzbeschreibung
- `octa_security/config_drift.ConfigBaseline` stellt ein unveränderliches Konfigurations-Baseline-Objekt bereit.
- Prüft auf: Hash‑Mismatch, numerische Parametergrenzen, fehlende Parameter.
- Erkennt Drift, löst Alarme aus und akzeptiert Änderungen nur mit expliziten Genehmigungen.

Designprinzipien
- Immutable baseline: Die Baseline wird beim Erzeugen gehasht und als unveränderlich behandelt.
- Keine stille Akzeptanz: Jede Abweichung wird protokolliert; Änderungen benötigen genehmigte Freigaben.
- Audit: Alle Prüfungen/Entscheidungen landen in `ConfigBaseline.audit_log`.

Benutzung (Kurz)
1. Erzeuge Baseline:

```py
from octa_security.config_drift import ConfigBaseline

baseline = {"max_trade_size": 1000, "symbols": ["EURUSD"]}
cb = ConfigBaseline(baseline, allowed_ranges={"max_trade_size": (0, 10000)}, required_approvals={"ops"})
```

2. Prüfen und erzwingen:

```py
current = {"max_trade_size": 20000, "symbols": ["EURUSD"]}
try:
    cb.enforce(current, approvals=["ops"], actor="ops_user")
except Exception as e:
    # handle alert / operator workflow
    print(e)
```

Audit & Evidence
- `cb.audit_log` enthält append‑only Ereignisse mit Timestamp, Actor, Action und Details.

Sicherheitshinweis
- Parameterrichtlinien (allowed_ranges) sollten restriktiv festgelegt werden.
- Änderungen an der Baseline müssen durch den Governance‑Prozess erfolgen und als neue Baseline mit neuem Hash eingeführt werden.
