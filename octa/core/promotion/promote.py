"""CLI: python -m octa.core.promotion.promote --run-dir <...> --symbol <...>

Output: promotion_decision.json to --evidence-dir (default: <run-dir>/promotion/)

No live-enable switch.  Purely reads artifacts and emits eligible_for_live bool.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .promotion_criteria import PromotionCriteria
from .promotion_engine import evaluate_promotion


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m octa.core.promotion.promote",
        description="Paper → Live promotion eligibility gate (read-only, no live activation).",
    )
    p.add_argument("--run-dir", required=True, help="Path to training run directory")
    p.add_argument("--symbol", required=True, help="Symbol to evaluate")
    p.add_argument(
        "--evidence-dir",
        default=None,
        help="Where to write promotion_decision.json (default: <run-dir>/promotion/)",
    )
    p.add_argument(
        "--sqlite-path",
        default="artifacts/registry.sqlite3",
        help="Path to ArtifactRegistry SQLite (default: artifacts/registry.sqlite3)",
    )
    p.add_argument(
        "--drift-registry-dir",
        default=None,
        help="Path to drift registry dir (default: no drift check)",
    )
    p.add_argument("--mode", default="paper", help="Mode tag (informational only; no effect on logic)")
    return p


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    run_dir = Path(args.run_dir)
    evidence_dir = (
        Path(args.evidence_dir)
        if args.evidence_dir
        else run_dir / "promotion"
    )
    sqlite_path = Path(args.sqlite_path) if args.sqlite_path else None
    drift_dir = Path(args.drift_registry_dir) if args.drift_registry_dir else None

    result = evaluate_promotion(
        args.symbol,
        run_dir=run_dir,
        criteria=PromotionCriteria(),
        sqlite_path=sqlite_path,
        drift_registry_dir=drift_dir,
        evidence_dir=evidence_dir,
    )

    summary = {
        "symbol": result.symbol,
        "eligible_for_live": result.eligible_for_live,
        "reasons": result.reasons,
        "decision_sha256": result.decision_sha256,
        "decision_path": result.decision_path,
    }
    print(json.dumps(summary, indent=2))
    return 0 if result.eligible_for_live else 1


if __name__ == "__main__":
    sys.exit(main())
