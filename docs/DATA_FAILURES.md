# Data Feed Failures & Degradation Mode

This document describes how OCTA handles market data outages and degraded feeds.

Principles
- Trading on stale data is forbidden.
- Degradation is explicit: when primary feed is stale but a fallback feed is fresh, the system enters degraded mode for that instrument.
- Recovery requires consecutive fresh reports from the primary feed before normal trading resumes.

Components
- `DataFeedManager` — register feeds, set per-instrument feed hierarchy, report updates, and query allowed trading.

Usage
```
from octa_ops.data_failures import DataFeedManager
dfm = DataFeedManager(freshness_seconds=5, recovery_required=2)
dfm.set_hierarchy("BTC", ["primary_feed", "backup_feed"]) 
dfm.report_update("backup_feed", "BTC")
allowed, reason = dfm.allow_trade("BTC", trade_type="exit")
```
