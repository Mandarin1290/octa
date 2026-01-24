**Spin‑Off & Modular Separation Protocol**

Zweck
- Ermöglicht die strukturierte Abspaltung (Spin‑Off) von Subsystemen ohne Schwächung des Kernsystems.

Prinzipien
- Spin‑Off darf die Core‑Integrität nicht beeinträchtigen: kritische Module bleiben im Kern.
- Abhängigkeiten müssen explizit deklariert und validierbar sein.
- Jede Abspaltung erzeugt ein manifestiertes Proposal, das überprüfbar und auditierbar ist.

Module (Beispiele)
- `risk_engine` — risikoberechnungen, kann unter Abhängigkeiten separiert werden.
- `execution_engine` — Order‑Routing; oft getrennte Compliance‑Anforderungen.
- `governance_framework` — Governance‑Logik; typischerweise kritisch und im Kern verbleibend.
- `monitoring_stack` — Monitoring und Telemetrie; kann als managed/extern laufen, aber braucht Governance.

Protokoll (Kurzablauf)
1. Registrierung aller Module mit `ModuleDescriptor(name, provides, depends_on, critical)`.
2. Erstellung eines Spin‑Off‑Vorschlags (`SpinOffManager.propose_spin_off([...])`).
3. Validierung: fehlende/verborgene Abhängigkeiten oder spin‑off kritischer Module führen zur Ablehnung.
4. Bei Annahme: Erzeuge serialisiertes Manifest mit Artefakten, Abhängigkeiten und evidence‑Hashes.

Kontrollen
- Externe Allowlist: explizit zugelassene externe Abhängigkeiten müssen vor der Abspaltung bestätigt werden.
- Escrow/contractual controls: Lizenz‑ und Escrow‑Klauseln bei Übergabe von Artefakten.

Audit
- Das erzeugte Proposal‑Manifest sollte in das Audit‑System eingespeist und signiert werden.
