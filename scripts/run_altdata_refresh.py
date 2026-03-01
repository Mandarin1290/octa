#!/usr/bin/env python3
"""AltData daily refresh — called by scripts/daily_refresh.sh.

Fetches macro-level AltData (FRED, COT, EIA, ECB, WorldBank, OECD, Stooq,
GDELT, …) using build_altdata_stack(allow_net=True).  Network access is
gated by _allow_net_effective() in orchestrator.py: requires both
OCTA_DAILY_REFRESH=1 (set by octa-daily-refresh.service) and allow_net=True.

After a successful fetch, writes a snapshot manifest via resolve_and_write()
so that training can verify provenance without any network access.

Training path is UNAFFECTED — config/altdat.yaml offline_only=true is
enforced by the feature_builder guard independently of this script.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

# Ensure project root on sys.path when invoked directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from octa.core.data.sources.altdata.orchestrator import build_altdata_stack
from octa.core.data.sources.altdata.snapshot_registry import resolve_and_write


def main() -> int:
    asof = date.today()
    run_id = "refresh_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    print(f"[altdata-refresh] start  run_id={run_id}  asof={asof}", flush=True)

    # Macro-only refresh: empty symbols list → skips per-symbol EDGAR/Reddit/etc.
    # Symbol-level EDGAR is fetched on-demand by the global gate during each run.
    result = build_altdata_stack(
        run_id=run_id,
        symbols=[],
        asof=asof,
        allow_net=True,
    )

    sources = result.get("sources", {})
    n_ok = sum(
        1 for v in sources.values()
        if isinstance(v, dict) and v.get("status") in {"ok", "fetched", "cached"}
    )
    n_total = len(sources)
    print(
        f"[altdata-refresh] fetch done  {n_ok}/{n_total} sources ok",
        flush=True,
    )

    # Write snapshot manifest (idempotent — skips if today's manifest already exists).
    snapshot_id, manifest_path = resolve_and_write(asof=asof)
    print(
        f"[altdata-refresh] snapshot  id={snapshot_id}  manifest={manifest_path}",
        flush=True,
    )

    # Write evidence pack.
    evidence_dir = (
        Path("octa") / "var" / "evidence"
        / f"altdata_refresh_{run_id.split('_', 1)[-1]}"
    )
    evidence_dir.mkdir(parents=True, exist_ok=True)
    (evidence_dir / "result.json").write_text(
        json.dumps(result, indent=2, default=str),
        encoding="utf-8",
    )
    (evidence_dir / "snapshot_id.txt").write_text(
        f"{snapshot_id}\n{manifest_path}\n",
        encoding="utf-8",
    )

    print(f"[altdata-refresh] evidence  dir={evidence_dir}", flush=True)

    # Non-zero exit only on complete failure (0 sources ok with sources present).
    if n_total > 0 and n_ok == 0:
        print(
            "[altdata-refresh] WARNING: all sources failed — check network / credentials",
            flush=True,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
