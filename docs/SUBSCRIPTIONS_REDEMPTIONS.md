# Subscriptions & Redemptions

This module implements subscription booking at NAV and a redemption queue that
respects available liquidity. It intentionally avoids forced liquidation.

APIs
----
- `SubscriptionManager.request_subscription(investor, share_class, amount)` —
  record a pending subscription to be executed at the next NAV.
- `SubscriptionManager.process_subscriptions(nav_price)` — execute pending
  subscriptions at the provided NAV price.
- `RedemptionManager.request_redemption(investor, share_class, shares)` —
  add redemption to FIFO queue.
- `RedemptionManager.process_redemptions(nav_price, available_liquid)` — process
  queued redemptions while available liquid cash permits; otherwise leave
  requests queued.

Capital flow events
-------------------
All operations emit `audit_fn` events where available: `subscription.requested`,
`subscription.processed`, `redemption.requested`, `redemption.queued`,
`redemption.processed`.

Behavioural notes
-----------------
- Subscriptions are executed strictly at NAV and convert cash to shares.
- Redemptions are only processed when sufficient liquid capital exists; if not,
  they remain queued (no forced sales or emergency liquidations).
