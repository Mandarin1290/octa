"""Promote a candidate model to the approved directory.

Copies the model file, generates manifest.json, signs with Ed25519,
and emits a MODEL_PROMOTED governance hash-chain event.

CLI usage::

    python -m octa.models.ops.promote \\
        --candidate octa/var/models/runs/<run>/<SYM>/<tf>/model.cbm \\
        --symbol AAPL --timeframe 1D \\
        --signing-key octa/var/keys/active_signing_key \\
        --out report.json
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from octa.core.governance.artifact_signing import (
    compute_sha256,
    sign_artifact,
)
from octa.core.governance.governance_audit import (
    EVENT_MODEL_PROMOTED,
    GovernanceAudit,
)

_DEFAULT_APPROVED_ROOT = Path("octa") / "var" / "models" / "approved"


def promote_model(
    *,
    candidate_path: Path,
    symbol: str,
    timeframe: str,
    signing_key_path: Path,
    approved_root: Path = _DEFAULT_APPROVED_ROOT,
    run_id: Optional[str] = None,
    thresholds: Optional[Dict[str, Any]] = None,
    registry: Optional[Any] = None,
    artifact_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Promote a candidate model to approved/.

    Returns a promotion report dict.
    """
    candidate_path = Path(candidate_path)
    if not candidate_path.exists():
        raise FileNotFoundError(f"Candidate model not found: {candidate_path}")
    if not signing_key_path.exists():
        raise FileNotFoundError(f"Signing key not found: {signing_key_path}")

    symbol = symbol.upper().strip()
    timeframe = timeframe.upper().strip()
    if not symbol or not timeframe:
        raise ValueError("symbol and timeframe must be non-empty")

    dest_dir = approved_root / symbol / timeframe
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_model = dest_dir / candidate_path.name

    # Atomicity marker: set PENDING_PROMOTION before file operations
    if registry is not None and artifact_id is not None:
        registry.set_lifecycle_status(artifact_id, "PENDING_PROMOTION")

    try:
        # Copy model
        shutil.copy2(str(candidate_path), str(dest_model))

        # Compute SHA-256
        digest = compute_sha256(dest_model)

        # Sign
        sha_path, sig_path = sign_artifact(dest_model, signing_key_path)

        # Write manifest
        ts = datetime.now(timezone.utc).isoformat()
        manifest = {
            "symbol": symbol,
            "timeframe": timeframe,
            "model_file": candidate_path.name,
            "sha256": digest,
            "promoted_at_utc": ts,
            "source_path": str(candidate_path),
            "thresholds": dict(thresholds or {}),
        }
        manifest_path = dest_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )

        # Emit governance event
        effective_run_id = run_id or f"promote_{symbol}_{timeframe}_{ts.replace(':', '').replace('-', '')}"
        gov = GovernanceAudit(run_id=effective_run_id)
        gov.emit(
            EVENT_MODEL_PROMOTED,
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "sha256": digest,
                "source": str(candidate_path),
                "dest": str(dest_model),
            },
        )

    except Exception:
        # Atomicity: mark failure so recovery tools can detect inconsistency
        if registry is not None and artifact_id is not None:
            try:
                registry.set_lifecycle_status(artifact_id, "PROMOTION_FAILED")
            except Exception:
                pass  # best-effort; do not shadow the original exception
        raise

    # Committed: update lifecycle status to PAPER
    if registry is not None and artifact_id is not None:
        registry.set_lifecycle_status(artifact_id, "PAPER")

    report = {
        "status": "promoted",
        "symbol": symbol,
        "timeframe": timeframe,
        "model_path": str(dest_model),
        "sha256": digest,
        "sig_path": str(sig_path),
        "sha256_path": str(sha_path),
        "manifest_path": str(manifest_path),
        "manifest": manifest,
        "governance_run_id": effective_run_id,
    }
    return report


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Promote a candidate model to approved/")
    ap.add_argument("--candidate", required=True, help="Path to candidate model file")
    ap.add_argument("--symbol", required=True, help="Symbol (e.g. AAPL)")
    ap.add_argument("--timeframe", required=True, help="Timeframe (e.g. 1D)")
    ap.add_argument("--signing-key", required=True, help="Path to Ed25519 signing key")
    ap.add_argument("--approved-root", default=str(_DEFAULT_APPROVED_ROOT))
    ap.add_argument("--run-id", default=None, help="Governance run ID")
    ap.add_argument("--out", default=None, help="Write report JSON to this path")
    args = ap.parse_args()

    report = promote_model(
        candidate_path=Path(args.candidate),
        symbol=args.symbol,
        timeframe=args.timeframe,
        signing_key_path=Path(args.signing_key),
        approved_root=Path(args.approved_root),
        run_id=args.run_id,
    )

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(report, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )

    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
