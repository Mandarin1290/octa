**Capital Accounts & Wallet Abstraction**

Kurzbeschreibung
- `octa_capital.accounts.CapitalAccount` stellt eine klare Trennung zwischen Trading‑Logik und rohem Kapital her.
- Trading‑Logik darf niemals `total_balance` direkt anpassen — alle Bewegungen sind explizit und protokolliert.

Konzepte
- `total`: gesamtes Kapital auf dem Account.
- `reserved`: per Strategie/Subaccount reserviertes Kapital.
- `deployable`: Kapital, das zur Reservierung/Allokation verfügbar ist (`total - sum(reserved)`).

Wesentliche Methoden
- `deposit(amount, actor)` — erhöht `total` und schreibt Audit.
- `reserve(subaccount, amount, actor)` — blockiert Kapital für eine Strategie (nur möglich, wenn `deployable` ausreichend ist).
- `release(subaccount, amount, actor)` — gibt reserviertes Kapital frei.
- `consume_reserved(subaccount, amount, actor)` — verwendet reserviertes Kapital (reduziert `reserved` und `total`).
- `transfer_between_subaccounts(from, to, amount, actor)` — bewegt Reservierungen zwischen Subaccounts.
- `get_balances()` — liefert `total`, `deployable` und `reserved` Aufschlüsselung.

Audit & Sicherheit
- `audit_log` ist append-only in‑memory (für production: schreibe in WORM/Audit‑Store).
- Alle fehlschlagenden Versuche werden ebenfalls protokolliert und lösen Exceptions.

Beispiel
```py
from octa_capital.accounts import CapitalAccount

acc = CapitalAccount("main")
acc.deposit(100000, actor="funding")
acc.reserve("strategy-1", 20000, actor="ops")
acc.consume_reserved("strategy-1", 5000, actor="execution")
print(acc.get_balances())
```
