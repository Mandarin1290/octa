# AltData Training Safety Audit — Phase 0–3

**Date:** 2026-02-28
**Status:** Complete (Phase 0–3)

## Background

Evidence `altdata_diag_20260228T184706Z` revealed that training could make live FRED
network calls when `FRED_API_KEY` was set in env but `offline_only` was absent from
`config/altdat.yaml`. This document records the investigation, fixes, and verification.

---

## Phase 0 — Discovery

**Finding:** Two separate AltData code paths exist:

| Path | Entry | Network risk |
|------|-------|-------------|
| Training sidecar | `features.py → sidecar.try_run → feature_builder.py` | **ACTIVE** (FRED when key set + offline_only absent) |
| Refresh (daily) | `build_altdata_stack(allow_net=True)` | Intended, isolated to `scripts/run_altdata_refresh.py` |

**Root cause:** `config/altdat.yaml` had no `offline_only` key → `altdat_cfg.get("offline_only", False)` → `False` → live fetch enabled whenever `FRED_API_KEY` present in env.

**Evidence files** (gitignored, local only):
- `octa/var/evidence/altdata_diag_20260228T184706Z/env_snapshot.txt`
- `octa/var/evidence/altdata_diag_20260228T184706Z/config_effective.json`
- `octa/var/evidence/altdata_diag_20260228T184706Z/altdata_sources_found.json`
- `octa/var/evidence/altdata_diag_20260228T184706Z/local_altdata_inventory.json`

---

## Phase 1A — Offline Mode Enforcement (commit `760cef2`)

**Change:** Added `offline_only: true` to `config/altdat.yaml` with explicit
Training/Refresh mode comment block.

**Effect:** `feature_builder.py:67` reads `offline_only=True` → routes FRED through
`read_snapshot(fallback_nearest=True)` instead of `fetch_fred_series()`.

**Tests:** `tests/test_altdata_training_offline_mode.py` (6 tests):
- Blocks live FRED network call even with `FRED_API_KEY` set
- Missing cache → `meta["error"] == "missing_cache"`, no exception raised
- Cache present → features loaded, no network call
- `config/altdat.yaml` has `offline_only: true` (regression guard)
- `OKTA_ALTDATA_OFFLINE_ONLY=1` env var override works

---

## Phase 1B — AltDataSnapshotRegistry (commit `e64335b`)

**New module:** `octa/core/data/sources/altdata/snapshot_registry.py`

Canonical snapshot manifest for reproducible training provenance. Records per-source
`sha256`, `row_count`, `stale_days`, and `cache_date` for every training run.

**API:**
```python
resolve_and_write(asof, *, cache_root, snapshots_root, sources) -> (snapshot_id, Path)
read_manifest(snapshot_id, *, snapshots_root) -> Optional[dict]
list_available_snapshots(*, snapshots_root) -> List[str]
get_latest_snapshot_id(*, snapshots_root) -> Optional[str]
```

**Manifest format:** `snapshot_id = YYYYMMDDThhmmssZ`
**Location:** `octa/var/altdata_snapshots/<snapshot_id>/manifest.json`
**Properties:** Idempotent, fail-soft, atomic write (`.tmp` rename), pure local I/O.

**Tests:** `tests/test_altdata_snapshot_registry.py` (12 tests).

---

## Phase 2 — Leakage Audit

**Verdict: NO_LEAKAGE_DETECTED**

### Feature-to-TF mapping

| Feature block | Prefix | Source | Config status | TF scope |
|--------------|--------|--------|---------------|----------|
| Macro | `altdat_macro_` | FRED | enabled | All TFs (meaningful at 1D/1H; NaN at 1m due to 6h tolerance) |
| EDGAR filings | `altdat_edgar_` | EDGAR | **disabled** | 1D only (if re-enabled) |
| Other sources | — | GDELT, Stooq, CoT, etc. | refresh-only | NOT in training path |

### Four independent leakage guards

1. **`cache._find_nearest_prior_date()`** — cache lookup ≤ asof date
2. **`time_sync.asof_join(direction="backward")`** — `pd.merge_asof` forward direction raises `ValueError`
3. **`time_sync.validate_no_future_leakage()`** — explicit `alt_ts > bar_ts` check, logged to `meta["leakage"]`
4. **`sidecar.try_run()` shift(1)** — all AltData features shifted 1 bar forward (conservative safety margin)

### Tolerance (max stale lag)

| TF | Tolerance | FRED daily coverage |
|----|-----------|-------------------|
| 1D | 45 days | Full (same-day obs ≤ bar close) |
| 1H | 7 days | Full |
| 30m | 3 days | Full |
| 5m | 1 day | Partial (14.5h lag at US open) |
| 1m | 6 hours | Effectively NaN (daily data always exceeds 6h) |

**Full report:** `octa/var/evidence/altdata_diag_20260228T184706Z/feature_alignment_report.json`

---

## Phase 3 — Test Runner Configs & Scripts (commit `782793e`)

**Configs added:**
- `configs/autopilot_test_50.yaml` — 50-symbol smoke test (runtime_profile: default, training_budget 1D:5/1H:5/30M:3/5M:2/1M:0, no network)
- `configs/autopilot_test_100.yaml` — 100-symbol smoke test (same budget, doubled pool, min_population: 50)

**Script:** `scripts/run_training_smoke_universe.py`
- Wraps `scripts/octa_autopilot.py` with given test config
- Parses run artifacts to write evidence pack:
  - `run_summary.md`, `gate_pass_rates.json`, `any_failures_top_reasons.json`, `proof_no_network.json`

**Invariants enforced in test configs:**
- `paper.enabled: false` — research context (prevents `IMMUTABLE_PROD_BLOCK`)
- `fred_enabled: false`, `edgar_enabled: false` — no live network
- AltData sidecar already protected by `offline_only: true` (Phase 1A)

---

## Test Counts

| Commit | New tests | Total suite |
|--------|-----------|-------------|
| Phase 1A (`760cef2`) | +6 | 1518 |
| Phase 1B (`e64335b`) | +12 | 1530 |
| Phase 3 (`782793e`) | 0 | 1530 |

All 1530 tests pass, 0 failed.
