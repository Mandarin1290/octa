# Incident Detection & Signal Aggregator

This module aggregates operational signals and produces incidents when deterministic, rule-based
conditions are met.

Rules
- Each signal type has a default numeric weight.
- A single very severe signal (weight >= IMMEDIATE_WEIGHT_THRESHOLD) creates an immediate incident.
- Otherwise signals are summed over a sliding window; total weight >= 11 triggers an incident.
- Multiple weak signals (>=3) within the window and total weight between 1 and 10 are promoted to trigger.
- Single low-weight signals are treated as noise and ignored.

Signals
- `execution_errors`, `latency_spike`, `risk_gate_violation`, `data_feed_anomaly`, `broker_api_failure`.

Usage
```
from octa_ops.detection import DetectionEngine
from octa_ops.incidents import IncidentManager

im = IncidentManager()
de = DetectionEngine(im)
de.ingest("engine", "latency_spike")
incidents = de.evaluate()
```
