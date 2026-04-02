#!/usr/bin/env python3
"""Model Promotion Pipeline: universe_1d PASS → registry PAPER.

Registers PASS-symbols from results.json into ArtifactRegistry
(artifacts/registry.sqlite3) with lifecycle_status=PAPER.

PKL paths point to octa/var/models/runs/universe_1d/stock/1D/<sym>/<sym>.pkl
(no file copy — paths are stored as-is in the registry).

Usage:
    python3 scripts/octa_promote_models.py [--dry-run]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

log = logging.getLogger("octa.promote")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def promote_models(
    run_dir: str = "octa/var/models/runs/universe_1d",
    results_file: str = "octa/var/models/runs/universe_1d/results.json",
    registry_root: str = "artifacts",
    level: str = "paper",
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Promote PASS-symbols from results.json into ArtifactRegistry.

    Idempotent: symbols already in registry at the given level are skipped.
    Returns summary dict with promoted/skipped/error counts.
    """
    from octa_ops.autopilot.registry import ArtifactRegistry

    results = json.loads(Path(results_file).read_text())
    pass_syms: List[str] = sorted(
        sym for sym, data in results.items()
        if data.get("1D", {}).get("status") == "PASS"
    )
    log.info(f"PASS symbols in results.json: {len(pass_syms)}")

    # Research context — write-allowed (not execution_service).
    reg = ArtifactRegistry(root=registry_root, ctx={"mode": "research"})

    already: set = {
        r["symbol"]
        for r in reg.get_promoted_artifacts(level=level)
        if r.get("timeframe") == "1D"
    }
    log.info(f"Already promoted at level={level}: {len(already)}")

    model_root = Path(run_dir) / "stock" / "1D"
    promoted: List[str] = []
    skipped: List[Tuple[str, str]] = []
    errors: List[Tuple[str, str]] = []

    for sym in pass_syms:
        if sym in already:
            skipped.append((sym, "already_promoted"))
            continue

        pkl = model_root / sym / f"{sym}.pkl"
        sha_file = model_root / sym / f"{sym}.sha256"

        if not pkl.exists():
            skipped.append((sym, "pkl_not_found"))
            log.warning(f"  ⚠ {sym}: PKL not found at {pkl}")
            continue

        # Use existing sidecar if present; recompute otherwise.
        if sha_file.exists():
            sha256 = sha_file.read_text().strip()
        else:
            sha256 = _sha256(pkl)

        if dry_run:
            log.info(f"  [dry-run] would promote {sym}: {pkl}")
            promoted.append(sym)
            continue

        try:
            artifact_id = reg.add_artifact(
                run_id="universe_1d",
                symbol=sym,
                timeframe="1D",
                artifact_kind="model",
                path=str(pkl),
                sha256=sha256,
                schema_version=1,
                status="ACTIVE",
            )
            reg.set_lifecycle_status(artifact_id, "PAPER")
            reg.promote(sym, "1D", artifact_id, level)
            promoted.append(sym)
            log.info(f"  ✅ {sym}: artifact_id={artifact_id} → PAPER")
        except Exception as exc:
            errors.append((sym, str(exc)))
            log.error(f"  ❌ {sym}: {exc}")

    result: Dict[str, Any] = {
        "promoted": len(promoted),
        "skipped": len(skipped),
        "errors": len(errors),
        "level": level,
        "dry_run": dry_run,
        "symbols": promoted,
        "error_details": errors,
    }
    log.info(
        f"Promotion complete: promoted={result['promoted']} "
        f"skipped={result['skipped']} errors={result['errors']}"
    )
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Promote PASS models to registry")
    parser.add_argument("--run-dir", default="octa/var/models/runs/universe_1d")
    parser.add_argument("--level", default="paper")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = promote_models(
        run_dir=args.run_dir,
        level=args.level,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2))
    return 0 if result["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
