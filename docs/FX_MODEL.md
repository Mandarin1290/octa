# FX Model

Overview
--------
This module models FX trading semantics consistent with institutional FX desks: two-legged trades, explicit funding/carry accrual, settlement exposure tracking, netting across strategies, and risk gates.

Files
-----
- `octa_assets/fx/pairs.py` — `FXPair` model and pip helpers.
- `octa_assets/fx/carry.py` — `CarryEngine` for accrual of funding/carry in quote currency.
- `octa_assets/fx/exposure.py` — `ExposureTracker` to track base and quote exposures and enforce caps.

Notes
-----
- FX trades are recorded as two legs: base exposure changes and opposite quote exposure changes.
- Carry accrual is explicit and must be applied daily to P&L (or into financing ledger).
- Netting aggregates exposures across strategies and accounts.
