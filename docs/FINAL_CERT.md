**Final Certification & Freeze**

Zweck
- Formelle Zertifizierung von OCTA Tier‑1 Completion. Die Zertifizierung friert den Kern ein und erfordert Governance‑Genehmigung für weitere Änderungen.

Inhalt der Zertifizierung
- `completion_hash`: deterministischer SHA‑256‑Hash über die System‑Provenance.
- `system_fingerprint`: Mapping subsystem -> evidence_hash oder manifest reference.
- `certified_scope`: Liste der Komponenten, die durch die Zertifizierung abgedeckt sind.
- `certified_at`: ISO‑Timestamp der Zertifizierung.
- `cert_hash`: deterministischer Hash des Zertifikats‑Manifestes.

Verfahren
- `FinalCertManager(provenance)` erstellt Manager mit übergebenen Evidenz‑Fingerprints.
- `certify(scope)` legt Zertifikat an und setzt `frozen=True`.
- Änderungen sind verboten solange `is_frozen()` true.
- Reopen: `request_reopen(requester, reason)` erzeugt eine Governance‑Anfrage; `approve_reopen(index, approver)` hebt Freeze nach Governance‑Approval auf.

Audit
- Reopen‑Requests und Approvals sind im Zertifikat als `reopen_requests` dokumentiert.
- Es wird empfohlen, das Zertifikats‑Manifest signiert zu veröffentlichen.
