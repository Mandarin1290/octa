**Internal Red‑Team Simulation**

Kurzbeschreibung
- `octa_chaos.red_team.RedTeamSimulator` erzeugt realistische internen Angriffe: `privilege_misuse`, `parameter_manipulation`, `timing_attack`.
- `octa_chaos.red_team.DefenseSystem` bietet testable Abwehr‑Primitiven mit append‑only `alerts`.

Designprinzipien
- Angriffe sind realistisch, aber sicher: Simulator ruft nur Verteidigungs‑APIs.
- Verteidigung arbeitet ohne vorherige Signatur: Regeln sind generische Heuristiken (approvals, parameter bounds, rate limits).
- Alerts: Jedes abgewiesene oder auffälliges Verhalten erzeugt ein `DefenseAlert` mit `ts`, `severity`, `actor`, `action`, `reason` und `details`.

Beispiel
```py
from octa_chaos.red_team import DefenseSystem, RedTeamSimulator

defense = DefenseSystem(baseline_privileges={"alice": ["ops"]}, allowed_ranges={"max_trade_size": (0,10000)}, rate_limit_per_minute=10)
red = RedTeamSimulator(defense)

# privilege misuse should be blocked
assert red.privilege_misuse("mallory", "alice", ["admin"]) is False
assert any(a.reason for a in defense.alerts)
```

Safety
- Tests must run in simulation mode only; the simulator never performs real network or production writes.
