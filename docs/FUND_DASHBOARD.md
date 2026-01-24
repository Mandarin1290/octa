# Fund Dashboard (Institutional View)

The `FundDashboard` provides a deterministic, ledger-reconcilable snapshot of
the fund for institutional consumption. It purposefully excludes marketing
metrics and only surfaces values derived from investor ledgers and provided
NAV prices.

Fields
------
- Fund AUM: sum of investor cash + share holdings valued at provided NAVs.
- NAV per share class: comes from an authoritative price input to the dashboard.
- Fee accruals: heuristic scan of ledger entries for `fee` markers.
- Investor capital balances: cash, per-class shares, market value and total.
- Liquidity & gate status: returns current `available_liquid` and gate allowance
  computed from a `RedemptionGateManager` if provided.

Reconciliation
--------------
All amounts use `Decimal` and are quantized to 8 decimal places. Use
`InvestorAccount.reconcile()` to confirm investor ledgers are consistent, and
compare `FundDashboard.fund_aum()` against independent ledger aggregation for
an end-to-end reconciliation.
