"""Approved-only model loader.

Execution MUST load models only from the approved directory.  Every load
verifies:

1. A ``manifest.json`` exists alongside the model.
2. The ``<model>.sha256`` digest matches.
3. The ``<model>.sig`` Ed25519 signature is valid.

If any check fails the loader returns a fail-closed rejection.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

from octa.core.governance.artifact_signing import compute_sha256, verify_artifact
from octa.core.governance.drift_monitor import is_disabled as _drift_is_disabled
from octa.core.governance.governance_audit import EVENT_MODEL_LOAD_REJECTED

if TYPE_CHECKING:
    from octa.core.governance.governance_audit import GovernanceAudit

_DEFAULT_APPROVED_ROOT = Path("octa") / "var" / "models" / "approved"


@dataclass(frozen=True)
class ModelLoadResult:
    ok: bool
    model_path: Optional[Path]
    manifest: Dict[str, Any]
    reason: str


def _read_manifest(manifest_path: Path) -> Dict[str, Any]:
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_approved_model(
    symbol: str,
    timeframe: str,
    *,
    public_key_path: Path,
    approved_root: Path = _DEFAULT_APPROVED_ROOT,
    model_filename: str = "model.cbm",
    drift_registry_dir: Optional[Path] = None,
    audit: Optional["GovernanceAudit"] = None,
) -> ModelLoadResult:
    """Load a model from the approved directory with full verification.

    The expected layout under ``approved_root`` is::

        <symbol>/<timeframe>/model.cbm
        <symbol>/<timeframe>/model.cbm.sha256
        <symbol>/<timeframe>/model.cbm.sig
        <symbol>/<timeframe>/manifest.json

    Parameters
    ----------
    symbol : str
        Ticker / symbol name.
    timeframe : str
        Timeframe identifier (e.g. "1D", "1H").
    public_key_path : Path
        Path to the Ed25519 public key for signature verification.
    approved_root : Path
        Root of the approved models directory.
    model_filename : str
        Name of the model file.

    Returns
    -------
    ModelLoadResult
        ``ok=True`` if all checks pass; ``ok=False`` with reason otherwise.
    """
    symbol_dir = approved_root / symbol.upper() / timeframe.upper()
    model_path = symbol_dir / model_filename
    manifest_path = symbol_dir / "manifest.json"

    if not model_path.exists():
        return ModelLoadResult(
            ok=False,
            model_path=None,
            manifest={},
            reason=f"model_not_found:{model_path}",
        )

    manifest = _read_manifest(manifest_path)
    if not manifest:
        return ModelLoadResult(
            ok=False,
            model_path=model_path,
            manifest={},
            reason=f"manifest_missing_or_invalid:{manifest_path}",
        )

    if not public_key_path.exists():
        return ModelLoadResult(
            ok=False,
            model_path=model_path,
            manifest=manifest,
            reason=f"public_key_not_found:{public_key_path}",
        )

    sig_ok = verify_artifact(model_path, public_key_path)
    if not sig_ok:
        return ModelLoadResult(
            ok=False,
            model_path=model_path,
            manifest=manifest,
            reason="signature_verification_failed",
        )

    # I2: cross-verify manifest sha256 against actual file (belt-and-suspenders)
    manifest_sha256 = manifest.get("sha256", "")
    if manifest_sha256:
        actual_sha256 = compute_sha256(model_path)
        if actual_sha256 != manifest_sha256:
            return ModelLoadResult(
                ok=False,
                model_path=model_path,
                manifest=manifest,
                reason="sha256_manifest_mismatch",
            )

    # I4: Drift enforcement — block load if model is in an active drift breach.
    if drift_registry_dir is not None:
        model_key = f"{symbol.upper()}_{timeframe.upper()}"
        if _drift_is_disabled(model_key, drift_registry_dir=drift_registry_dir):
            if audit is not None:
                audit.emit(
                    EVENT_MODEL_LOAD_REJECTED,
                    {
                        "reason": "drift_disabled",
                        "model_key": model_key,
                        "symbol": symbol,
                        "timeframe": timeframe,
                    },
                )
            return ModelLoadResult(
                ok=False,
                model_path=model_path,
                manifest=manifest,
                reason="drift_disabled",
            )

    return ModelLoadResult(
        ok=True,
        model_path=model_path,
        manifest=manifest,
        reason="approved",
    )


def list_approved_models(
    approved_root: Path = _DEFAULT_APPROVED_ROOT,
) -> list[Dict[str, Any]]:
    """List all models in the approved directory."""
    if not approved_root.exists():
        return []
    results = []
    for symbol_dir in sorted(approved_root.iterdir()):
        if not symbol_dir.is_dir():
            continue
        for tf_dir in sorted(symbol_dir.iterdir()):
            if not tf_dir.is_dir():
                continue
            manifest_path = tf_dir / "manifest.json"
            manifest = _read_manifest(manifest_path)
            results.append({
                "symbol": symbol_dir.name,
                "timeframe": tf_dir.name,
                "manifest": manifest,
                "has_model": (tf_dir / "model.cbm").exists(),
                "has_sig": (tf_dir / "model.cbm.sig").exists(),
                "has_sha256": (tf_dir / "model.cbm.sha256").exists(),
            })
    return results
