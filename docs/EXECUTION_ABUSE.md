Execution Abuse Simulation

Purpose

Simulate abusive execution scenarios (order floods, cancel/replace loops, exchange rejection storms) and verify that the OMS enforces throttling and exposure caps.

Design

- `OrderManagementSystem` implements per-strategy throttling (`max_orders_per_sec`) and exposure capping (`exposure_cap`). It records fills idempotently and keeps an audit log.
- `ExchangeSimulator` can be configured with a `reject_rate` to emulate exchange rejection storms.

Hard rules

- No duplicate exposure: fills are recorded idempotently by `order_id`.
- OMS must throttle and reject when limits exceeded.

Usage

- Use `OrderManagementSystem.receive_order(...)` to submit orders; check `accepted` flag and `reason` if rejected.
- Use `ExchangeSimulator.send_order(order)` to simulate exchange response and call `OrderManagementSystem.record_fill(...)` for fills.
