Data Poisoning & Corruption Attacks

Purpose

Simulate corrupted and malicious market data feeds and verify that detection systems block corrupted feeds before execution and activate fallbacks.

Attack types

- Delayed timestamps: feed timestamps lag behind wall-clock time.
- Price spikes: sudden large jumps that are inconsistent with recent history.
- Silent drift: gradual drift over time that can bias strategies.

Design

- `DataFeed` models a feed with `price`, `ts`, and `history` of recent updates.
- `DataPoisoningSimulator` injects corruption: `delayed_timestamp`, `price_spike`, `silent_drift`.
- `DetectionEngine` runs detectors for timestamp delay, spikes and silent drift; thresholds are configurable.
- `MarketExecutionGuard` validates primary feed and selects the freshest non-corrupted fallback if needed.

Hard rules

- Poisoned data must never reach execution: the guard raises if no healthy fallback exists.
- Detection must precede trading decisions: validate feeds before using prices.

Usage

Create feeds, optionally inject corruption with the simulator, then call `MarketExecutionGuard.select_feed(primary, fallbacks)` to obtain an execution feed.
