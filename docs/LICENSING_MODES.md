**LICENSING & DEPLOYMENT MODES**

Zweck
- Formalisiert zulässige Lizenz‑ und Bereitstellungsmodi für OCTA, wobei Kernintegrität erhalten bleibt und unkontrollierte Forks verhindert werden.

Modi (Kurzbeschreibung)
- `internal_fund_only`: Nur interner Einsatz innerhalb eines Fonds/Organisation. Kein externer Codezugriff oder Redistribution.
- `licensed_engine`: Lizenzen an Dritte; Engine wird als Artefakt (binär/obfusk) geliefert; vertraglich geregelte Nutzungsrechte; keine freien Forks.
- `managed_service`: Betreiber stellt Service; Kunden konsumieren über API; kein Zugriff auf Quellcode.
- `white_label_restricted`: Kunden bekommen Branding/Deployment unter strikten Bedingungen; keine Quellcode‑Weitergabe; Escrow/Watermarking.

Harte Regeln (immer gültig)
- Kernintegrität bewahren: Releases sind signiert, Evidence‑Hashes werden veröffentlicht, Audit‑Logs bleiben append‑only.
- Keine unkontrollierten Forks: Quellcodefreigaben erfordern vertragliche Bedingungen, Code‑Escrow oder explizite Freigabe.

Matrix (Inhalte)
- Für jeden Modus sind definiert: `source_access`, `allow_forks`, `commercial_use`, `redistribution`, `support_model`, `restrictions`.
- Das Modul `octa_legal/licensing_matrix.py` liefert die maschinenlesbare Matrix und eine `assess_request`‑Funktion zur schnellen Prüfung von Anfragen.

Enforcement Empfehlungen
- Signierte Release‑Artefakte: Jede verteilte Version muss einen `evidence_hash` und signierte Release‑Manifestdateien enthalten.
- Code Escrow: Bei lizenzierter Source‑Übergabe sollte ein Escrow‑Mechanismus vorliegen.
- Provenance: Artefakte sollten Metadaten/Wasserzeichen enthalten, die Herkunft und Version belegen.
- Audit Sharing: Regelmäßige, signierte Exporte von Audit‑Logs an berechtigte Prüfer.

Beispiel: Anforderungsprüfung
- Die `assess_request(mode_key, requested_actions)`‑Funktion gibt sofort zurück, ob angefragte Operationen (z. B. `fork`, `redistribute_source`) mit dem ausgewählten Modus kompatibel sind.

Nächste Schritte (Empfohlen)
- Ergänzung: Vertragsvorlagen für No‑Fork, Escrow‑Verträge und SLAs für `managed_service`.
- Tooling: Release‑Signing und automatische Manifest‑Publikation für Auditoren.
