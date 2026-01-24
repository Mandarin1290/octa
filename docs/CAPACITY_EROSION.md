# Capacity Erosion Monitor

Purpose
-------
Detect degrading execution quality for strategies (capacity erosion) and suggest dynamic capital scaling.

Signals
-------
- `slippage` (absolute or relative; higher is worse)
- `impact` (market impact estimate; higher is worse)
- `fill_ratio` (fraction of intended volume filled; lower is worse)

Design
------
- Maintain append-only history per strategy.
- Compute long-window (baseline) and short-window (recent) means.
- Compare recent vs baseline using configurable percentage thresholds.
- Aggregate component scores into a conservative erosion score.
- Suggest a reduced capacity = current_capacity * (1 - erosion_score * max_reduction).
- Emit auditable alerts with canonical evidence hashes.

API
---
- `CapacityErosionMonitor.record_metrics(date, strategy, slippage, impact, fill_ratio)`
- `CapacityErosionMonitor.set_capacity(strategy, capacity)`
- `CapacityErosionMonitor.get_capacity(strategy)`
- `CapacityErosionMonitor.evaluate(strategy) -> Optional[ErosionAlert]`

Notes
-----
- Thresholds and windows are configurable per monitor instance.
- The monitor is conservative: it averages three signals and requires multiple samples to avoid reacting to noise.
