Investor Reports
================

Overview
--------
`investor_reports.generate_investor_report()` produces an investor-grade historical report including:

- Performance summary: total and annualized returns (period-based)
- Risk metrics: annualized volatility (from daily returns) and maximum drawdown
- Fees: summed and reconciled with a deterministic hash

Hard Rules
----------
- No forward-looking promises: report contains an explicit disclaimer and only uses historical inputs.
- Metrics reconcile with NAV inputs: `nav_end` equals the last NAV in the input series and fees are summed from provided fee records.

Inputs
------
- `nav_series`: ordered list of objects with `date` (ISO string) and `nav` (NAV-per-share float). Must contain at least one point.
- `fee_records`: list of objects with `date`, `type`, and `amount`.

Output
------
Returns a `Report` dataclass with reconciled metrics and a `reconciliation_hash` computed using canonical JSON + SHA-256.

Usage Notes
-----------
- Caller is responsible for providing authoritative NAV and fee inputs (these should be derived from the NAV engine and fee engine to ensure reconciliation).
- The `reconciliation_hash` can be stored alongside audit evidence to prove the report inputs produced the output.
