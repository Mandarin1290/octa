**IP Asset Registry**

Kurzbeschreibung:
- Registriert IP‑Einheiten als versionierte, unveränderliche Assets.
- Bietet eindeutige Asset‑IDs, Versionen mit unveränderlicher Lineage, Abhängigkeitsgraphen und Lifecycle‑Status (active/deprecated).

API Übersicht:
- `IPRegistry()` — Registry‑Instanz.
- `add_asset(name, asset_id=None, description=None) -> asset_id` — legt neues Asset an; `asset_id` kann vorgegeben oder generiert werden.
- `add_version(asset_id, version, parent=None, metadata=None, dependencies=None) -> evidence_hash` — fügt eine unveränderliche Version hinzu; dependencies sind Strings `asset_id@version`.
- `get_version(asset_id, version) -> Version` — liest Versionen.
- `deprecate_version(asset_id, version)` — markiert Version als `deprecated`.
- `dependency_graph() -> Dict[str,List[str]]` — liefert Abhängigkeitskanten als Map `asset@ver -> [asset@ver,...]`.
- `topological_sort() -> List[str]` — liefert topologisch sortierte Knoten oder wirft bei Zyklen.
- `verify_lineage(asset_id, version) -> bool` — prüft, ob Beweishash und Elternkette konsistent sind.

Designprinzipien:
- Jede Version ist immutable: einmal angelegt, darf sie nicht verändert werden.
- Abhängigkeiten müssen auf existierende Asset‑Versionen zeigen.
- Einfügen von Versionen, die Zyklen im Abhängigkeitsgraphen erzeugen, wird abgelehnt.
- Jede Version erhält einen deterministischen `evidence_hash` (SHA‑256 über canonical JSON) zur Auditierbarkeit.

Beispiel:

1. Asset anlegen:

```
from octa_ip.ip_registry import IPRegistry
reg = IPRegistry()
aid = reg.add_asset("strategy_alpha", asset_id="alpha")
```

2. Baseline‑Version hinzufügen:

```
hash_v1 = reg.add_version("alpha", "v1", metadata={"notes": "initial"})
```

3. Neue Version mit Parent und Abhängigkeit:

```
hash_v2 = reg.add_version("alpha", "v2", parent="v1", dependencies=["libx@1.2.0"])
```

Hinweis zur Persistenz:
- Dieses Modul stellt nur die In‑Memory‑API bereit. Für Produktionsbetrieb wird empfohlen, Registry‑Snapshots regelmäßig zu persistieren (z. B. signierte JSON‑Manifeste oder Datenbank) und die `evidence_hash` extern zu signieren.
