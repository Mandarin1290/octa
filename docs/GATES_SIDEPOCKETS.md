# Gates, Lockups & Side Pockets

This document describes investor protection mechanisms implemented in the
`octa_fund` package.

Gates
-----
- `RedemptionGateManager` enforces a percentage cap of AUM that can be
  redeemed in a single processing window (e.g. 5% of AUM).

Lockups
-------
- `LockupManager` records time‑based lockups per `investor_id` and `share_class`.
- Locked shares cannot be redeemed until the lock expires.

Side pockets
------------
- `SidePocketManager` tracks pocketed (illiquid) shares separately from the
  main redeemable pool. Moving shares into a pocket reduces the available
  main shares used for normal redemptions. Redemption from a pocket requires
  explicit approval.

Audit
-----
These components are designed to be wired into the larger `audit_fn`
framework; managers themselves do not persist audit events but can be
integrated into orchestration that forwards events into `AuditChain`.
