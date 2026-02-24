"""Key rotation utilities for Ed25519 signing keys.

Provides:
- check_rotation_due(): is the key past its rotation interval?
- rotate_key(): generate new keypair, re-sign approved models, emit governance events
- load_rotation_policy(): parse key_rotation section from policy.yaml
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

_DEFAULT_INTERVAL_DAYS = 90
_DEFAULT_ALERT_DAYS_BEFORE = 7


def check_rotation_due(
    signing_key_path: Path,
    *,
    interval_days: int = _DEFAULT_INTERVAL_DAYS,
) -> Dict[str, Any]:
    """Check whether a signing key is past its rotation interval.

    Uses the file's mtime as a proxy for key creation date (conservative:
    actual creation is never later than mtime).

    Returns
    -------
    dict with keys:
    - due: bool — True if rotation is overdue
    - key_age_days: int — age in whole days
    - interval_days: int — configured interval
    - key_mtime: str — ISO timestamp of key mtime
    - days_until_due: int — negative if overdue
    - error: str — set if key file not found (due=False)
    """
    signing_key_path = Path(signing_key_path)
    if not signing_key_path.exists():
        return {
            "due": False,
            "key_age_days": None,
            "interval_days": interval_days,
            "key_mtime": None,
            "days_until_due": None,
            "error": f"key_not_found:{signing_key_path}",
        }

    stat = signing_key_path.stat()
    mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    age_days = (now - mtime).days
    days_until_due = interval_days - age_days

    return {
        "due": age_days >= interval_days,
        "key_age_days": age_days,
        "interval_days": interval_days,
        "key_mtime": mtime.isoformat(),
        "days_until_due": days_until_due,
    }


def rotate_key(
    *,
    old_private_path: Path,
    old_public_path: Path,
    new_private_path: Path,
    new_public_path: Path,
    run_id: str,
    approved_root: Optional[Path] = None,
    auto_resign: bool = True,
) -> Dict[str, Any]:
    """Generate a new Ed25519 keypair and optionally re-sign all approved models.

    Emits EVENT_KEY_ROTATED and EVENT_KEY_REVOKED to the governance hash chain.

    Parameters
    ----------
    old_private_path : Path
        Path to the existing private signing key (must exist).
    old_public_path : Path
        Path to the existing public verification key.
    new_private_path : Path
        Destination path for the new private key.
    new_public_path : Path
        Destination path for the new public key.
    run_id : str
        Governance run ID for audit events.
    approved_root : Path, optional
        Root of approved models directory. If provided and auto_resign=True,
        all *.cbm files will be re-signed with the new private key.
    auto_resign : bool
        If True and approved_root is set, re-sign all approved models.

    Returns
    -------
    dict with rotation report.
    """
    from octa.core.governance.artifact_signing import generate_keypair, sign_artifact
    from octa.core.governance.governance_audit import (
        EVENT_KEY_REVOKED,
        EVENT_KEY_ROTATED,
        GovernanceAudit,
    )

    old_private_path = Path(old_private_path)
    old_public_path = Path(old_public_path)
    new_private_path = Path(new_private_path)
    new_public_path = Path(new_public_path)

    if not old_private_path.exists():
        raise FileNotFoundError(f"Old private key not found: {old_private_path}")

    # Generate new keypair
    generate_keypair(new_private_path, new_public_path)

    # Re-sign approved models with new private key
    resigned: List[str] = []
    resign_errors: List[str] = []
    if auto_resign and approved_root is not None:
        approved_root = Path(approved_root)
        if approved_root.exists():
            for model_path in sorted(approved_root.rglob("*.cbm")):
                try:
                    sign_artifact(model_path, new_private_path)
                    resigned.append(str(model_path))
                except Exception as exc:
                    resign_errors.append(f"{model_path}: {exc}")

    # Emit governance events
    gov = GovernanceAudit(run_id=run_id)
    gov.emit(
        EVENT_KEY_ROTATED,
        {
            "new_public_key": str(new_public_path),
            "old_public_key": str(old_public_path),
            "resigned_model_count": len(resigned),
            "resign_errors": resign_errors,
        },
    )
    gov.emit(
        EVENT_KEY_REVOKED,
        {
            "revoked_public_key": str(old_public_path),
        },
    )

    return {
        "status": "rotated",
        "new_private_path": str(new_private_path),
        "new_public_path": str(new_public_path),
        "resigned_models": resigned,
        "resign_errors": resign_errors,
        "governance_run_id": run_id,
    }


def load_rotation_policy(policy_path: Path) -> Dict[str, Any]:
    """Load key rotation policy from a YAML config file.

    Reads the ``key_rotation`` section. Falls back to defaults if the file
    does not exist or the section is missing.

    Returns
    -------
    dict with keys: interval_days, alert_days_before, auto_resign_on_rotation
    """
    defaults: Dict[str, Any] = {
        "interval_days": _DEFAULT_INTERVAL_DAYS,
        "alert_days_before": _DEFAULT_ALERT_DAYS_BEFORE,
        "auto_resign_on_rotation": True,
    }
    try:
        import yaml  # type: ignore[import]
        policy_path = Path(policy_path)
        if not policy_path.exists():
            return dict(defaults)
        raw = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
        kr = (raw or {}).get("key_rotation", {})
        return {**defaults, **(kr or {})}
    except Exception:
        return dict(defaults)
