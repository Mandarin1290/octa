# Regulatory & Market-Practice Rules

Overview
--------
This module enforces configurable regulatory and market-practice rules as pre-trade checks. Violations are treated as regulatory breaches and can trigger immediate freezes via the Sentinel.

Initial Rules
-------------
- Short-selling locate required: selling more than current position requires a locate.
- No naked shorts: if no locate and shorting beyond position, block and freeze.
- Pattern-risk detection: excessive cancels (cancel storms) detected per account/instrument.
- Max order frequency per instrument: configurable per-window limit; exceedance triggers pattern risk.
- Wash-trade prevention: detect self-crosses (same account, opposite side, same price) within the recent window; regulatory breach.

Integration
-----------
Use `octa_vertex.pretrade_regulatory.PreTradeRegulator` in the pre-trade path. Provide `positions_lookup(account_id, instrument)` and `locates_lookup(account_id, instrument)` callables to give live position and locate state.

Configuration
-------------
Example config keys:

- `require_locate` (bool, default true)
- `cancel_threshold` (int)
- `cancel_window_seconds` (int)
- `max_order_freq` (int)
- `order_freq_window_seconds` (int)

On regulatory violation, `Sentinel.set_gate(3, reason)` will be called to freeze trading. Pattern risk events use gate level 2 by default.
