AltData Live Placement
======================

Overview
--------
This document describes the live AltData placement across the 1D → 1H → 30M → 5M → 1M cascade, the freshness policy, and fail-closed behavior.

Gates and Sources
-----------------
1D Global Regime (global_1d)
- Sources: FRED, ECB/SDW, OECD, WorldBank, COT, EIA, GDELT, Google Trends, Wikipedia, Reddit.
- Usage: regime/risk context (macro risk, event stress, positioning risk, attention).
- Safety: AltData can only reduce risk (never force risk-on).

1H Signal/Context (signal_1h)
- Sources: GDELT, Reddit, Google Trends, Yahoo earnings proximity, optional FinGPT event classify (if healthy).
- Usage: confidence modifiers / confirmations only (never standalone triggers).
- Safety: high event stress or proximity to earnings suppresses confidence.

30M Structure (structure_30m)
- Sources: COT alignment, GDELT stress, Yahoo corporate action guard.
- Usage: setup validation and corporate action risk guard.
- Safety: high event stress or corporate action proximity invalidates setups.

5M Execution (execution_5m)
- Sources: GDELT stress, optional Reddit spike.
- Usage: volatility guard (reduce/skip execution).
- Safety: high stress blocks entries.

1M Micro (micro_1m)
- Sources: Reddit spikes / Trends attention (optional).
- Usage: order optimization hints only.
- Safety: never overrides risk limits.

Freshness Policy
----------------
- Daily sources: valid for 24h.
- Weekly sources (COT): valid until next release (default 7 days).
- Hourly/medium sources: 6–24h (configurable).
- If stale or missing: features are dropped and gates treat them as missing (fail-closed).

Fail-Closed Behavior
--------------------
- Missing or stale sources never enable risk-on behavior.
- If Yahoo/FinGPT unavailable, outputs are neutral and gate logic continues safely.
- All new sources write audit logs:
  - `octa/var/audit/altdata_live/`
  - `octa/var/audit/yahoo_refresh/`
  - `octa/var/audit/fingpt/`

Configuration
-------------
See `config/altdata_live.yaml` for:
- Enabled sources and per-source refresh/timeout/TTL settings.
- FinGPT toggle and circuit breaker.
- Yahoo enablement and cache TTL.
- Gate overlay thresholds (bounded, conservative).
