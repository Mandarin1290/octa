# Trade Surveillance & Market Abuse Controls

Purpose: automated, independent surveillance for market abuse indicators. Alerts are informational and must not trigger trading actions automatically.

Hard rules:

- Surveillance is independent from strategy logic.
- Alerts are not actioned (no auto-execution).
- System prefers false positives over false negatives.

Patterns detected:

- `spoofing_like`: many small orders followed by high cancellation ratio.
- `layering_like`: orders placed at multiple price levels then cancelled.
- `abnormal_cancellations`: unusually high cancellation rate in window.
- `wash_trade_indicator`: fills by same actor on both sides within short time.

Usage:

- Use `SurveillanceEngine.ingest(event)` to feed observed order events. Events are simple dicts containing `id`, `actor`, `instrument`, `side`, `price`, `qty`, `type` (`new`/`cancel`/`fill`) and `ts`.
- Inspect `SurveillanceEngine.alert_log` for alerts (deterministic, timestamped).
- Tune thresholds via constructor parameters.

Notes:

- Alerts should be triaged by human analysts; integrate with incident workflows to create incidents for S2+ patterns.
- Persist alerts to append-only storage for audit and evidence generation.
