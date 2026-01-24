# Investor Due‑Diligence Pack Generator

Purpose: generate a reproducible, factual due‑diligence pack that is reconcilable with system logs and audit evidence.

Content:

- `system_architecture`: discovered `octa_` components.
- `risk_framework`: registered control objectives and mapping to controls.
- `governance`: model inventory and approval states.
- `incidents`: deterministic incident history snapshot.
- `recovery`: recovery progress summary.
- `ops_snapshot` (optional): aggregated ops dashboard snapshot.

Usage:

- Call `create_dd_pack(...)` with the relevant managers to produce a `DDPack` object.
- Use `export_dd_pack_json()` to serialize for archival; compute hash is embedded and reproducible from the snapshot.

Reconciliation:

- The pack includes counts and lists (e.g., incidents) that must match managers' logs. Use `incident_manager.list_incidents()` and audit evidence to reconcile.

Notes:

- The generator produces factual, non-promotional content — do not add marketing language.
- Persist exported packs to an append-only store for investor review.
