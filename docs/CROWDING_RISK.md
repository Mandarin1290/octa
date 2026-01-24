**Crowding & Concentration Risk**

Overview
- Prevent hidden duplication and factor concentration across strategies and assets.

Key Components
- `ExposureGraph` (`octa_core.concentration`): nodes and weighted edges represent strategy exposures to assets/asset-classes.
- Metrics: Herfindahl index (HHI), top-N contribution, factor proxy concentration.
- Actions: recommended allocator scaling and sentinel gate events when thresholds are breached.

Integration
- Feed portfolio exposures each rebalancing into `ExposureGraph` and call `evaluate_concentration`.
- Persist snapshots with `octa_ledger.exposure_graph.ExposureLedger.append_snapshot` for audit/provenance.

Design Notes
- Factor proxies are expected as maps asset->loading; missing proxies trigger conservative caps.
- Duplicate detection uses cosine-similarity over exposure vectors to find hidden replication.
