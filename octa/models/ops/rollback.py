"""Auto-rollback: retire a drifting model and restore the champion.

Called by the drift monitor when a model exceeds its breach threshold.
champion.json schema::

    {
        "symbol": "AAPL",
        "timeframe": "1D",
        "champion_model_dir": "/path/to/champion/files",
        "artifact_id": 123,          // optional — used to set lifecycle LIVE
        "sha256": "abc...",          // optional — sanity check
        "set_at": "2026-01-01T..."
    }

champion_model_dir must contain the same layout as the approved dir::

    model.cbm
    model.cbm.sha256
    model.cbm.sig
    manifest.json
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from octa.core.governance.artifact_signing import verify_artifact
from octa.core.governance.governance_audit import (
    EVENT_GOVERNANCE_ENFORCED,
    GovernanceAudit,
)

_DEFAULT_APPROVED_ROOT = Path("octa") / "var" / "models" / "approved"
_MODEL_FILENAME = "model.cbm"
_SIDECAR_SUFFIXES = (".sha256", ".sig")
_MANIFEST_FILENAME = "manifest.json"


@dataclass(frozen=True)
class RollbackResult:
    ok: bool
    reason: str
    symbol: str
    timeframe: str
    retired_path: Optional[Path]
    champion_model_dir: Optional[Path]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts_for_path() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_champion_json(champion_json_path: Path) -> Optional[Dict[str, Any]]:
    if not champion_json_path.exists():
        return None
    try:
        return json.loads(champion_json_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def execute_rollback(
    *,
    symbol: str,
    timeframe: str,
    champion_json_path: Path,
    approved_root: Path = _DEFAULT_APPROVED_ROOT,
    public_key_path: Optional[Path] = None,
    registry: Optional[Any] = None,
    artifact_id: Optional[int] = None,
    audit: Optional[GovernanceAudit] = None,
) -> RollbackResult:
    """Retire the current approved model and restore the champion.

    Fail-closed: if champion.json is missing or corrupt, or the champion
    model files are absent, returns ok=False without touching the current
    approved model.

    Parameters
    ----------
    symbol, timeframe:
        Identify the model slot (case-insensitive; normalised to upper).
    champion_json_path:
        Path to the champion.json record that points to the champion model dir.
    approved_root:
        Root of the approved models directory.
    public_key_path:
        Ed25519 public key for champion signature verification.  If None,
        signature verification is skipped (test / dry-run scenarios).
    registry:
        Optional ArtifactRegistry instance.  When provided, the current
        artifact is marked RETIRED; the champion artifact_id (from champion.json)
        is marked LIVE.
    artifact_id:
        artifact_id of the currently deployed model (to mark RETIRED).
    audit:
        Optional GovernanceAudit.  When provided, an EVENT_GOVERNANCE_ENFORCED
        record is appended with reason="drift_rollback".
    """
    symbol_up = symbol.upper().strip()
    tf_up = timeframe.upper().strip()

    # ── 1. Load champion.json — fail-closed if missing/corrupt ──────────────
    champion_cfg = _load_champion_json(champion_json_path)
    if champion_cfg is None:
        return RollbackResult(
            ok=False,
            reason="champion_json_missing_or_corrupt",
            symbol=symbol_up,
            timeframe=tf_up,
            retired_path=None,
            champion_model_dir=None,
        )

    champion_dir_raw = champion_cfg.get("champion_model_dir", "")
    if not champion_dir_raw:
        return RollbackResult(
            ok=False,
            reason="champion_model_dir_not_set",
            symbol=symbol_up,
            timeframe=tf_up,
            retired_path=None,
            champion_model_dir=None,
        )

    champion_dir = Path(champion_dir_raw)

    # ── 2. Champion model must exist — fail-closed if missing ────────────────
    champion_model = champion_dir / _MODEL_FILENAME
    if not champion_model.exists():
        return RollbackResult(
            ok=False,
            reason="champion_model_missing",
            symbol=symbol_up,
            timeframe=tf_up,
            retired_path=None,
            champion_model_dir=champion_dir,
        )

    # ── 3. Optionally verify champion signature ──────────────────────────────
    if public_key_path is not None and public_key_path.exists():
        if not verify_artifact(champion_model, public_key_path):
            return RollbackResult(
                ok=False,
                reason="champion_signature_invalid",
                symbol=symbol_up,
                timeframe=tf_up,
                retired_path=None,
                champion_model_dir=champion_dir,
            )

    # ── 4. Move current approved → retired/<ts>/<SYMBOL>/<TF>/ ──────────────
    current_dir = approved_root / symbol_up / tf_up
    retired_path: Optional[Path] = None
    if current_dir.exists():
        retired_path = approved_root / "_retired" / _ts_for_path() / symbol_up / tf_up
        retired_path.mkdir(parents=True, exist_ok=True)
        for item in current_dir.iterdir():
            shutil.move(str(item), str(retired_path / item.name))

    # ── 5. Copy champion files → approved/<SYMBOL>/<TF>/ ────────────────────
    current_dir.mkdir(parents=True, exist_ok=True)
    for item in champion_dir.iterdir():
        shutil.copy2(str(item), str(current_dir / item.name))

    # ── 6. Registry lifecycle updates ────────────────────────────────────────
    if registry is not None:
        if artifact_id is not None:
            try:
                registry.set_lifecycle_status(artifact_id, "RETIRED")
            except Exception:
                pass  # best-effort; rollback already executed
        champion_artifact_id = champion_cfg.get("artifact_id")
        if champion_artifact_id is not None:
            try:
                registry.set_lifecycle_status(int(champion_artifact_id), "LIVE")
            except Exception:
                pass

    # ── 7. Governance event ──────────────────────────────────────────────────
    if audit is not None:
        audit.emit(
            EVENT_GOVERNANCE_ENFORCED,
            {
                "reason": "drift_rollback",
                "symbol": symbol_up,
                "timeframe": tf_up,
                "champion_model_dir": str(champion_dir),
                "retired_path": str(retired_path) if retired_path else None,
                "champion_artifact_id": champion_cfg.get("artifact_id"),
                "retired_artifact_id": artifact_id,
                "timestamp_utc": _utc_now_iso(),
            },
        )

    return RollbackResult(
        ok=True,
        reason="rollback_complete",
        symbol=symbol_up,
        timeframe=tf_up,
        retired_path=retired_path,
        champion_model_dir=champion_dir,
    )


def save_champion_record(
    *,
    symbol: str,
    timeframe: str,
    champion_model_dir: Path,
    champion_json_path: Path,
    artifact_id: Optional[int] = None,
    sha256: Optional[str] = None,
) -> None:
    """Write a champion.json record pointing to the saved champion model dir.

    Called by the promotion system when a new model supersedes the current
    champion — the outgoing model is archived as the new champion backup.
    """
    champion_json_path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "symbol": symbol.upper(),
        "timeframe": timeframe.upper(),
        "champion_model_dir": str(champion_model_dir),
        "artifact_id": artifact_id,
        "sha256": sha256,
        "set_at": _utc_now_iso(),
    }
    champion_json_path.write_text(
        json.dumps(record, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
