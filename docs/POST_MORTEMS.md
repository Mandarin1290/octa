**Post‑Mortem Automation (Blameless, Evidence‑First)**

Kurzbeschreibung
- `octa_ops.postmortem.generate_postmortem()` erstellt automatisierte, vorlagebasierte Post‑Mortems.
- Regeln: blameless (keine Schuldzuweisungen), evidence‑based (hashes + snapshots), improvement‑focused (konkrete Remediationschritte).

Inhalt eines Reports
- `timeline`: geordnete Ereignisse
- `root_cause`: heuristisch ermittelt (gegebenenfalls manuell anpassbar)
- `failed_safeguards`: aufgelistete Schutzmechanismen, die versagt haben
- `remediation_actions`: priorisierte, konkrete Schritte
- `evidence`: deterministische Hashes für alle bereitgestellten Beweise

Beispiel
```py
from octa_ops.postmortem import generate_postmortem

incident = {
    "id": "inc-123",
    "events": [{"ts": "2025-12-28T10:00:00Z", "message": "price feed stale", "type": "warning"}],
    "failed_safeguards": ["stale_data"],
    "evidence": {"feed_snapshot": {"last": 1}}
}

report = generate_postmortem(incident)
print(report.to_dict())
```

Hinweis
- Der generierte `root_cause` ist ein Ausgangspunkt für die Untersuchung; menschliche Review ergänzt technische Befunde.
