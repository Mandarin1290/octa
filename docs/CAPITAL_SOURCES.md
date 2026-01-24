**Capital Sources: Internal vs Investor Pools**

Kurzbeschreibung
- `octa_capital.capital_sources.CapitalSources` trennt internes Kapital von Investor‑(externem) Kapital und sorgt für explizite, protokollierte Bewegungen.

Wesentliche Regeln
- Externes Kapital beeinflusst niemals Strategie‑Entscheidungen: `strategy_deployable()` liefert ausschließlich internes Deployable.
- Alle Deposits/Allocations sind explizit und werden in `audit_log` aufgezeichnet.

API‑Highlights
- `deposit_internal(amount, actor)` — fügt interne Mittel hinzu.
- `deposit_investor(investor_id, amount, actor)` — legt/incrementiert Investor‑Account.
- `allocate_to_strategy(subaccount, amount, actor)` — reserviert nur aus internem Pool (für Entscheidungen & Execution).
- `allocate_from_investor(investor_id, subaccount, amount, actor)` — reserviert Investor‑Mittel (nur für execution/settlement; nicht für decision signals).
- `aggregate_view()` — liefert getrennte Buchhaltung über interne und Investor‑Pools.

Empfehlung
- Strategiecode MUSS `strategy_deployable()` nutzen, nicht `aggregate_view()`, um Entscheidungen zu treffen.
