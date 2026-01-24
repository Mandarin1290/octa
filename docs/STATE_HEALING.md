**State Healing & Corruption Resilience**

Kurzbeschreibung
- `octa_resilience.state_healing.StateManager` bietet snapshot-basierte, deterministische Wiederherstellung gegen interne Zustandskorruption.
- Szenarien: in-memory Korruption, stale Cache, partielle Persistenzfehler.

Prinzipien
- Korruption darf nicht propagiert werden: stale oder nicht-verifizierte Daten werden verworfen.
- Deterministische Recovery: Wiederherstellung erfolgt aus einem unveränderlichen Snapshot mit stabiler Serialisierung (canonical JSON + SHA-256).
- Audit: Append-only `audit_log` dokumentiert alle Erkennungen und Wiederherstellungen.

Benutzung (Kurz)
```py
from octa_resilience.state_healing import StateManager, PersistenceSimulator, CacheSimulator

initial = {"positions": {"A": 100}, "cash": 100000}
mgr = StateManager(initial)

# simulate corruption
mgr.state["cash"] = 0
if mgr.detect_corruption():
    mgr.heal()

# persist a new state, failing persistence
mgr.persistence.fail_next_write = True
mgr.persistence.partial_write = {"positions": {"A": 100}}
try:
    mgr.persist_state({"positions": {"A": 50}, "cash": 80000})
except Exception:
    # deterministic rollback occurred
    pass
```

Empfehlungen
- Speichere Snapshots in einem WORM/Audit-Store für forensische Wiederherstellung.
- Halte Cache- und Persistenz-Schnittstellen prüfbar und instrumentiert.
