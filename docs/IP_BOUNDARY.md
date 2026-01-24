# OCTA — Intellectual Property Boundary Definition

Ziel
----
Dieses Dokument definiert verbindlich, welche Bestandteile von OCTA als schutzwürdiges geistiges Eigentum (IP) gelten, wie sie von operativen Assets getrennt werden und welche Nachweis‑ und Governanceprozesse für Audit und Bewertung einzuhalten sind.

Grundprinzipien
---------------
- Trennung: IP ist klar vom laufenden Betrieb (Daten, kurzfristige Konfigurationen, Kunden‑Secrets) zu trennen.
- Nachvollziehbarkeit: Jedes IP‑Element muss eine kanonische Evidence‑Hash und Metadaten haben.
- Minimalismus: Nur werthaltige, wiederverwendbare Artefakte werden als IP deklariert.
- Governance: Änderungen am IP‑Katalog bedürfen Governance‑Freigabe und Audit‑Einträgen.

IP‑Kategorien (Beispiele)
-------------------------
- Kernalgorithmen: Signale, Modellarchitekturen, Verlustfunktionen, proprietäre Anpassungen an ML‑Algorithmen.
- Architektur‑Patterns: Wiederverwendbare Systemarchitekturen, Schnittstellen‑Contracts, resilient‑design primitives.
- Governance‑Logik: Cutover‑Protokolle, fee‑crystallization Regeln, cutover/rollback decision trees mit Evidenzanforderungen.
- Tools & Pipelines: Orchestrationsskripte, deterministische serialiserungs‑/hashing‑routinen, audit‑evidence builders.

Nicht‑IP / Operative Assets
--------------------------
- Rohdaten, Marktdaten, Kundendaten, Secrets/Keys — diese sind nicht IP, sondern regulierte Assets.
- Konfigurationswerte, kurzfristige Thresholds, per‑deployment Credentials.

Grenzziehungskriterien
----------------------
Ein Artefakt ist IP, wenn alle folgenden zutreffen:

1. Wiederverwendbarkeit: Das Artefakt wird in mehreren Deployments, Strategien oder Produkten nutzbar sein.
2. Innovationsbeitrag: Das Artefakt enthält handschriftliche, nicht‑triviale Anpassungen oder Designs.
3. Ökonomischer Wert: Das Artefakt trägt messbar zur Performance oder Kosteneffizienz bei.
4. Trennbarkeit: Das Artefakt kann getrennt entnommen, dokumentiert und versioniert werden.

Kennzeichnung & Nachweis
------------------------
- Jedes IP‑Element bekommt:
  - Eindeutigen Namen und Kategorie.
  - Autor/Owner (Team / Legal entity).
  - Version und Change‑History (konservativ append‑only).
  - Canonical evidence hash (JSON canonicalization + SHA‑256).
  - Kurzbeschreibung mit Abgrenzung zur Betriebsdaten‑Sicht.

Governance‑Prozesse
-------------------
- Aufnahme: Vorschlag → technische Review → Legal/Valuation Review → Governance Freigabe (Audit‑Record erstellt).
- Änderung: Jede Änderung an IP erfordert neue Version und erneute Freigabe; Abwärtskompatibilität ist zu dokumentieren.
- Entnahme / Lizenzierung: Jede Herausgabe (extern, Lizenz, M&A) dokumentiert mit Evidence‑Hash und Genehmigungs‑Audit.

Audit & Bewertung
-----------------
- Evidenz‑Hashes werden in `ContinuousAudit` als snapshots abgespeichert.
- Für Bewertungen (Valuation) werden: code size, test‑coverage, performance‑benchmarks und evidence‑history zusammengeführt.
- Externe Prüfungen: Produktion einer `IP Manifest` Datei (machine‑readable, signierbar) für Due‑Diligence.

Rechte & Lizenzen
-----------------
- Standard: OCTA‑internes Eigentum; Lizenzbedingungen werden für externe Verwendungen durch Legal vorgegeben.
- Open‑Source‑Ausnahmen sind explizit zu kennzeichnen (Name, Version, License, SPDX Tag).

Kontrollen zur Vermeidung von Vermischung
-----------------------------------------
- Entwicklertools und Repos müssen Trennung von Daten/credentials und IP‑Artefakten erzwingen.
- CI pipelines erzeugen signierte artifacts und aktualisieren IP‑Manifest only after governance approval.

Notfallverfahren
----------------
- Falls operativer Code irrtümlich als IP deklariert wurde, wird ein Governance‑Ticket erstellt, Evidence archiviert und die Deklaration nach Review ggf. zurückgezogen.

Appendix: Beispiele
-------------------
- `octa_strategy/alpha_decay.py` → Kategorie: `core_algorithms` (evidence_hash, tests, docs)
- `octa_core/cutover.py` → Kategorie: `governance_logic` (protocol, irreversible steps)
- `octa_audit/continuous_audit.py` → Kategorie: `tools` (canonical hashing, snapshots)
