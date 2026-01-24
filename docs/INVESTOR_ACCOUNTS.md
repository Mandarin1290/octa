# Investor Accounts (Logical Model)

Purpose
-------
This module models investor-level capital accounts using a ledger-first approach.

Key rules
---------
- Each investor has an independent `InvestorAccount` instance.
- All capital movements are recorded as immutable ledger entries.
- Capital flows do not affect strategy logic; this is accounting-only.

Concepts
--------
- `InvestorAccount`: holds a cash `balance`, a map of `shares` by share class,
  and an append-only ledger of `LedgerEntry` items.
- `LedgerEntry`: represents a single atomic change (cash inflow/outflow,
  share purchase/sale) with `amount` (cash effect), `details` and `timestamp`.

Behavior
--------
- Use `deposit()` and `withdraw()` to change cash.
- Use `buy_shares()` and `sell_shares()` to change both cash and share ownership.
- Every operation appends a ledger entry and calls an optional `audit_fn(event,payload)`.
- Use `reconcile()` to validate that the on-object balance and shares match
  the ledger-derived totals.

Determinism & Auditing
----------------------
All monetary values use `Decimal` and quantize to 8 decimal places for deterministic
comparisons. `timestamp`s are recorded in timezone-aware UTC ISO format.

Examples
--------
Create an account, deposit cash, buy shares, reconcile:

```python
from decimal import Decimal
from octa_fund.investor_accounts import InvestorAccount

acct = InvestorAccount('inv-1', 'Alice')
acct.deposit(Decimal('100'))
acct.buy_shares('class-A', Decimal('3'), Decimal('10'))
assert acct.reconcile()
```
