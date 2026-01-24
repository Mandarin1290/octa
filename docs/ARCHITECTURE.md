OCTA Foundation - Architecture and allowed import graph

Module boundaries (high-level):

- `octa_core`: primitives, types, time, ids, events. No imports from other octa packages.
- `octa_fabric`: configuration and dependency wiring. May import `octa_core`.
- `octa_stream`: data ingestion contracts and validation. May import `octa_core` and `octa_fabric` (config only).
- `octa_atlas`: artifact and model registry contracts. May import `octa_core` and `octa_fabric` (config only).
- `octa_sentinel`: risk gate and kill-switch. May import `octa_core` and `octa_fabric`.
- `octa_vertex`: execution/OMS contracts. May import `octa_core`, `octa_fabric`, and `octa_sentinel` (risk checks only). Must NOT import `octa_stream` or `octa_atlas` directly.
- `octa_ledger`: audit trail primitives. May import `octa_core` and be used by `octa_nexus` for global recording.
- `octa_nexus`: orchestrator. Responsible for wiring modules together via explicit interfaces; may import all other modules but must only call interfaces, preserving boundaries.

Forbidden direct imports (enforced by tests):

- `octa_vertex` must not import `octa_stream` or `octa_atlas`.
- Research/training modules (not present here) must not import `octa_vertex`.

Design notes:

- Separation of concerns prevents execution code from reading raw datasets. Validated inference outputs flow through `octa_stream` -> `octa_atlas` and only registered artifacts are visible to execution via `octa_atlas`'s read-only interfaces.
- All modules expose typed contracts (Protocols or ABCs) to enable testing and real integrations later.
