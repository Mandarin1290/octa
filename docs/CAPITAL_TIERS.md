# Capital Tiers

Overview
--------
Capital tiers classify the firm's capitalisation level and influence risk, capacity and strategy eligibility.

Defined Tiers
-------------
- `SEED` — small AUM; conservative defaults.
- `GROWTH` — medium AUM; relaxed limits.
- `INSTITUTIONAL` — large AUM; most permissive defaults.

Configuration
-------------
The `CapitalTierEngine` accepts configurable thresholds (`seed_max`, `growth_max`) and a mapping of `TierMultipliers`:
- `max_leverage` — multiplier for leverage limits
- `max_participation` — permitted market participation fraction
- `max_concentration` — permitted per-strategy concentration

Integration
-----------
- Instantiate `CapitalTierEngine` with `audit_fn` wired to `AuditChain.append` to persist transition logs.
- Attach to `AUMState` via `engine.attach(aum_state)` to receive snapshot-driven tier updates.
- Use `engine.multipliers_for(engine.get_current_tier())` during allocation and capacity calculations.

Audit
-----
Transitions are emitted as `capital.tier.transition` audit events with payload: `timestamp`, `previous_tier`, `new_tier`, `aum_total`.
