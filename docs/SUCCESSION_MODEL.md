**Succession & Long‑Term Maintenance Model (10+ years)**

Zweck
- Definiert Verantwortlichkeiten, Übergabeprozesse und Notfall‑Custodianship, um den Betrieb von OCTA über 10+ Jahre sicherzustellen.

Harte Regeln
- Kein Key‑Person‑Dependency: Schlüsselaufgaben müssen mindestens doppelt besetzt oder von Gremien/trustees abgesichert sein.
- Governance‑Kontinuität: Ownership‑Übergaben erfordern dokumentierte Approvals und werden auditierbar manifestiert.

Hauptbestandteile
- Ownership: primäre Eigentümer/Operatoren, ergänzt durch Trustees zur Risiko‑Abfederung.
- Trustees: unparteiische, langfristig verpflichtete Akteure (z. B. Rechtsstelle, Risikovorstand).
- Custodians: operative Hände, die Zugang zu Systemen haben (Operator‑Team). Emergency Custodians sind extern designierte Stellen, die zeitlich begrenzt übernehmen können.

Übergabeprozesse
- Geplante Übergabe: 1) Erstellung eines `SuccessionPlan`, 2) Scheduling der Transition mit Approvers, 3) Ausführung und Auditlog‑Eintrag, 4) Export signierten Manifests.
- Operator Handover: Checklisten (credentials rotation, evidence export, runbooks, operational keys) sind verpflichtend.

Emergency Custodianship
- Benennung: Jede kritische Rolle muss mindestens einen Emergency‑Custodian haben.
- Trigger: Ein Emergency‑Trigger erzeugt ein kurzlebiges Handover‑Manifest (z. B. 30 Tage), das in Audit‑Logs aufgenommen wird.

Ownership Transitions
- Transitions müssen approvers enthalten (z. B. 2 of 3 trustees) und dürfen nicht die Mindestanzahl an Owners unterschreiten.
- Transfers werden manifestiert und mit einem deterministischen Evidence‑Hash versehen.

Operational Controls
- Credential Rotation: Automatisierte Rotation nach Übergabe; secrets in KMS/escrow.
- Runbooks: ausgefüllte Runbooks und checklists werden als Teil des Manifestes archiviert.
- Audit: Alle Schritte sind in einem append‑only Audit gespeichert und verifizierbar.

Governance Continuity
- Regelmäßige Übungen: Handover‑Drills mindestens jährlich.
- Dokumentation: Aktuelle Contact‑Lists, Escrow‑Verträge und SLAs müssen gepflegt werden.

Empfehlung
- Implementiere automatisierte Manifest‑Signing (Release signatures) und periodische Offsite‑Backups der Audit‑Logs und Manifeste.
