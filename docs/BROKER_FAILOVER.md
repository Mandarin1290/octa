# Broker Failure & Failover Playbook

This module provides conservative, auditable handling of broker connectivity failures.

Principles
- Broker loss must be survivable: pending orders are re-routed to healthy brokers when possible.
- No duplicate orders: client_order_id uniqueness is enforced across the system.
- Failover does not increase exposure: re-routing preserves intended exposure effects and avoids double-applying orders.

Components
- `BrokerHealthMonitor` — register brokers and check heartbeat; failed brokers identified via a failure threshold.
- `BrokerFailoverManager` — place orders, record fills, perform failover routing for pending orders, and reconcile external states.

Typical flow
1. Monitor detects broker `X` failed.
2. `BrokerFailoverManager.failover("X")` attempts to re-route pending orders to eligible healthy brokers.
3. The manager enforces client_order_id uniqueness and avoids exposure increases.
4. Reconciliation can be run using `reconcile_order_states()` with external broker states.

Limitations
- This module provides conservative, repository-level logic; production deployments should integrate broker APIs, transactional guarantees, and idempotent order submission with broker-side client IDs.
