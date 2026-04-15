#!/usr/bin/env python3
"""fix_altdata_snapshots.py — Backfill FRED snapshots with full historical time series.

Root cause fixed:
  Current snapshots are "seed" files with 1 data point per series.
  build_macro_features() produces only constant _lvl features;
  all _chg_1/_roc_20/_z_252 features are NaN → zero signal.

This script:
  1. Reads full 2000-2026 time series from DuckDB fred_series table
     for FEDFUNDS, DGS10, DGS2, UNRATE (already cached there).
  2. Fetches CPIAUCSL, CPILFESL, T10YIE from FRED API (key required).
  3. Optionally fetches Tier-B series: T10Y2Y, BAMLH0A0HYM2, STLFSI2.
  4. Writes a new snapshot dated today in the exact format expected by
     feature_builder.py:
       octa/var/altdata/fred/<today>/fred_<today>.json
       {"series": {"FEDFUNDS": [{"ts": "...", "value": ...}, ...], ...}}
  5. Validates that build_macro_features() produces non-NaN outputs.

Usage:
    python octa/support/altdata/fix_altdata_snapshots.py [--dry-run] [--skip-api]

Constraints:
    - Does NOT touch the running retrain process.
    - Does NOT modify training code.
    - Uses write_snapshot() from cache.py for atomic writes.
    - Training reads this snapshot on next run (fallback_nearest=True).
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

# Project root on path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

DUCKDB_PATH = Path("octa/var/altdata/altdat.duckdb")
ALTDATA_ROOT = "octa/var/altdata"

# 4 series with full DuckDB history (2000–2026)
DUCKDB_SERIES = ["FEDFUNDS", "DGS10", "DGS2", "UNRATE"]

# 3 series missing from DuckDB — fetched from FRED API
FRED_API_SERIES = ["CPIAUCSL", "CPILFESL", "T10YIE"]

# Tier-B series: yield curve + credit + stress (optional, fetched via API)
FRED_TIER_B = ["T10Y2Y", "BAMLH0A0HYM2", "STLFSI2"]

FRED_START = "2000-01-01"


# ---------------------------------------------------------------------------
# DuckDB reader
# ---------------------------------------------------------------------------

def read_duckdb_series(series_ids: list[str]) -> dict[str, list[dict]]:
    """Read deduplicated time series from DuckDB fred_series table."""
    try:
        import duckdb
    except ImportError:
        print("  [WARN] duckdb not installed — skipping DuckDB read")
        return {}

    if not DUCKDB_PATH.exists():
        print(f"  [WARN] DuckDB not found at {DUCKDB_PATH}")
        return {}

    db = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    result: dict[str, list[dict]] = {}

    for sid in series_ids:
        # Deduplicate: per ts keep latest ingested_at row
        rows = db.execute("""
            SELECT ts, value
            FROM (
                SELECT ts, value, ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY ts
                        ORDER BY ingested_at DESC NULLS LAST
                    ) AS rn
                FROM fred_series
                WHERE series_id = ?
                  AND value IS NOT NULL
            ) sub
            WHERE rn = 1
            ORDER BY ts
        """, [sid]).fetchall()

        if rows:
            result[sid] = [
                {
                    "ts": r[0].isoformat() if hasattr(r[0], "isoformat") else str(r[0]),
                    "value": float(r[1]),
                }
                for r in rows
                if r[1] is not None
            ]
            print(
                f"  DuckDB  {sid:20s}: {len(result[sid]):6,} rows "
                f"({result[sid][0]['ts'][:10]} → {result[sid][-1]['ts'][:10]})"
            )
        else:
            print(f"  DuckDB  {sid:20s}: 0 rows — SKIPPING")

    db.close()
    return result


# ---------------------------------------------------------------------------
# FRED API fetcher
# ---------------------------------------------------------------------------

def fetch_api_series(
    series_ids: list[str],
    api_key: str,
    start: str = FRED_START,
) -> dict[str, list[dict]]:
    """Fetch time series from FRED API for series not in DuckDB."""
    result: dict[str, list[dict]] = {}

    try:
        import pandas as pd
        from octa.core.data.sources.altdata.fred_connector import fetch_fred_series
    except ImportError as e:
        print(f"  [WARN] import failed: {e}")
        return result

    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(date.today().isoformat(), tz="UTC")

    for sid in series_ids:
        res = fetch_fred_series(
            series_id=sid,
            start_ts=start_ts,
            end_ts=end_ts,
            api_key=api_key,
        )
        if res.ok and res.df is not None and not res.df.empty:
            rows = [
                {"ts": str(row["ts"]), "value": float(row["value"])}
                for _, row in res.df.iterrows()
                if row.get("value") is not None
            ]
            if rows:
                result[sid] = rows
                print(
                    f"  FRED API {sid:20s}: {len(rows):6,} rows "
                    f"({rows[0]['ts'][:10]} → {rows[-1]['ts'][:10]})"
                )
            else:
                print(f"  FRED API {sid:20s}: 0 rows after filter")
        else:
            err = getattr(res, "error", "unknown") or "fetch failed"
            print(f"  FRED API {sid:20s}: FAILED — {err}")

    return result


# ---------------------------------------------------------------------------
# Snapshot writer
# ---------------------------------------------------------------------------

def write_fred_snapshot(
    series_data: dict[str, list[dict]],
    dry_run: bool = False,
) -> Path:
    """Write snapshot in the format expected by feature_builder.py."""
    from octa.core.data.sources.altdata.cache import write_snapshot

    today = date.today()
    payload = {"series": series_data}
    meta = {
        "source": "fred",
        "asof": today.isoformat(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "generator": "fix_altdata_snapshots.py",
        "seed": False,
        "series_count": len(series_data),
        "rows_per_series": {k: len(v) for k, v in series_data.items()},
    }

    if dry_run:
        print(f"\n[dry-run] would write snapshot for {today}:")
        for k, v in series_data.items():
            first = v[0]["ts"][:10] if v else "?"
            last = v[-1]["ts"][:10] if v else "?"
            print(f"  {k:20s}: {len(v):6,} rows ({first} → {last})")
        return Path("(dry run)")

    payload_path, _meta_path, sha = write_snapshot(
        source="fred",
        asof=today,
        payload=payload,
        meta=meta,
        root=ALTDATA_ROOT,
    )
    print(f"\n  Snapshot written: {payload_path}")
    print(f"  SHA256:          {sha[:32]}...")
    return payload_path


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

def validate_snapshot(series_data: dict[str, list[dict]]) -> dict:
    """Validate that build_macro_features() produces non-NaN outputs."""
    try:
        import pandas as pd
        from octa.core.features.transforms.macro_features import build_macro_features
        from octa.core.data.sources.altdata.fred_connector import (
            FredFetchResult,
            fred_to_wide,
        )
    except ImportError as e:
        return {"ok": False, "error": f"import: {e}"}

    results = []
    for sid, rows in series_data.items():
        df = pd.DataFrame(rows)
        if not df.empty:
            df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
            df = df.dropna(subset=["ts"])
        results.append(
            FredFetchResult(series_id=sid, df=df, ok=bool(len(df)), error=None)
        )

    wide = fred_to_wide(series=results)
    if wide.empty:
        return {"ok": False, "error": "fred_to_wide returned empty"}

    macro = build_macro_features(wide)
    if macro.empty:
        return {"ok": False, "error": "build_macro_features returned empty"}

    chg_cols = [c for c in macro.columns if "_chg_1" in c]
    roc_cols = [c for c in macro.columns if "_roc_20" in c]
    z_cols = [c for c in macro.columns if "_z_252" in c]
    lvl_cols = [c for c in macro.columns if "_lvl" in c]

    def valid_count(cols: list[str]) -> int:
        return sum(int(macro[c].notna().sum()) for c in cols)

    return {
        "ok": True,
        "total_rows": len(macro),
        "total_features": len(macro.columns),
        "lvl_valid_values": valid_count(lvl_cols),
        "chg_1_valid_values": valid_count(chg_cols),
        "roc_20_valid_values": valid_count(roc_cols),
        "z_252_valid_values": valid_count(z_cols),
        "z_252_non_nan_pct": (
            round(valid_count(z_cols) / max(len(macro) * len(z_cols), 1) * 100, 1)
            if z_cols else 0.0
        ),
        "series": list(series_data.keys()),
        "feature_cols": len(macro.columns),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print plan, do not write")
    parser.add_argument("--skip-api", action="store_true", help="Skip FRED API calls (use DuckDB only)")
    parser.add_argument("--skip-tier-b", action="store_true", help="Skip Tier-B series (T10Y2Y/BAMLH/STLFSI2)")
    args = parser.parse_args()

    print("=" * 60)
    print(" ALTDATA SNAPSHOT FIX")
    print("=" * 60)
    print(f" Mode:     {'DRY RUN' if args.dry_run else 'WRITE'}")
    print(f" DuckDB:   {DUCKDB_PATH}")
    print(f" Root:     {ALTDATA_ROOT}")
    print()

    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key and not args.skip_api:
        print("[WARN] FRED_API_KEY not set — will skip FRED API series")
        args.skip_api = True

    # Step 1: Read DuckDB
    print("Step 1 — Reading DuckDB (FEDFUNDS/DGS10/DGS2/UNRATE):")
    series_data = read_duckdb_series(DUCKDB_SERIES)

    # Step 2: Fetch missing Tier-A series from FRED API
    if not args.skip_api:
        print("\nStep 2 — Fetching Tier-A series from FRED API (CPIAUCSL/CPILFESL/T10YIE):")
        api_data = fetch_api_series(FRED_API_SERIES, api_key)
        series_data.update(api_data)
    else:
        print("\nStep 2 — FRED API skipped (--skip-api or no key)")

    # Step 3: Fetch Tier-B series
    if not args.skip_api and not args.skip_tier_b:
        print("\nStep 3 — Fetching Tier-B series (T10Y2Y/BAMLH0A0HYM2/STLFSI2):")
        tier_b_data = fetch_api_series(FRED_TIER_B, api_key)
        series_data.update(tier_b_data)
    else:
        print("\nStep 3 — Tier-B skipped")

    if not series_data:
        print("\n[ERROR] No series data collected — aborting")
        return 1

    total_rows = sum(len(v) for v in series_data.values())
    print(f"\n  Total: {len(series_data)} series, {total_rows:,} rows")

    # Step 4: Validate
    print("\nStep 4 — Validating features:")
    val = validate_snapshot(series_data)
    if val.get("ok"):
        print(f"  total_rows:          {val['total_rows']:,}")
        print(f"  feature_cols:        {val['feature_cols']}")
        print(f"  _lvl valid values:   {val['lvl_valid_values']:,}")
        print(f"  _chg_1 valid values: {val['chg_1_valid_values']:,}  (was 0)")
        print(f"  _roc_20 valid:       {val['roc_20_valid_values']:,}  (was 0)")
        print(f"  _z_252 valid:        {val['z_252_valid_values']:,}  (was 0)")
        print(f"  _z_252 coverage:     {val['z_252_non_nan_pct']:.1f}% non-NaN")
    else:
        print(f"  [WARN] Validation issue: {val.get('error')}")

    # Step 5: Write snapshot
    print("\nStep 5 — Writing snapshot:")
    snapshot_path = write_fred_snapshot(series_data, dry_run=args.dry_run)

    # Step 6: Summary
    print()
    print("=" * 60)
    print(" RESULT")
    print("=" * 60)
    missing = [s for s in DUCKDB_SERIES + FRED_API_SERIES if s not in series_data]
    for sid, rows in sorted(series_data.items()):
        first = rows[0]["ts"][:10] if rows else "?"
        last = rows[-1]["ts"][:10] if rows else "?"
        print(f"  {sid:20s}: {len(rows):6,} rows ({first} → {last})")
    if missing:
        for sid in missing:
            print(f"  {sid:20s}: FEHLT/KEIN_KEY")

    if val.get("ok"):
        chg_fixed = val["chg_1_valid_values"] > 0
        z_fixed = val["z_252_valid_values"] > 0
        print()
        if chg_fixed and z_fixed:
            print("  _chg_1/_roc_20/_z_252: JA → NEIN (NaN eliminiert)")
            print("  RETRAIN BEREIT: JA")
        else:
            print("  WARN: some features still NaN — check series coverage")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
