"""Deterministic training fingerprint for smart checkpoint skip.

Computes a TRAINING_FINGERPRINT from:
  - code_version (git SHA)
  - normalized config hash
  - symbol + timeframe
  - training window (start, end)
  - data fingerprint (lightweight: row count + column hash)
  - anchored global_end

Policy:
  match   => SKIP_CHECKPOINT_HIT (reuse existing artifact)
  mismatch => retrain required
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


def _git_sha() -> str:
    """Get current git commit SHA (or UNKNOWN)."""
    try:
        cp = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, check=False,
        )
        sha = (cp.stdout or "").strip()
        return sha if sha else "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def _stable_json_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class DataFingerprint:
    """Lightweight data fingerprint without reading all rows."""
    row_count: int
    column_hash: str  # hash of sorted column names
    first_date: Optional[str]
    last_date: Optional[str]


@dataclass(frozen=True)
class TrainingFingerprint:
    code_version: str
    config_hash: str
    symbol: str
    timeframe: str
    window_start: str
    window_end: str
    global_end: str
    data_fingerprint_hash: str
    fingerprint_hash: str  # overall composite hash


@dataclass(frozen=True)
class CheckpointDecision:
    action: str  # "SKIP_CHECKPOINT_HIT" or "RETRAIN"
    reason: str
    current_fingerprint: str
    stored_fingerprint: Optional[str]


def compute_data_fingerprint(
    *,
    row_count: int,
    columns: Sequence[str],
    first_date: Optional[str] = None,
    last_date: Optional[str] = None,
) -> DataFingerprint:
    """Compute a lightweight data fingerprint."""
    col_hash = hashlib.sha256(
        ",".join(sorted(str(c) for c in columns)).encode("utf-8")
    ).hexdigest()[:16]
    return DataFingerprint(
        row_count=row_count,
        column_hash=col_hash,
        first_date=first_date,
        last_date=last_date,
    )


def compute_training_fingerprint(
    *,
    config: Mapping[str, Any],
    symbol: str,
    timeframe: str,
    window_start: str,
    window_end: str,
    global_end: str,
    data_fingerprint: DataFingerprint,
    code_version: Optional[str] = None,
) -> TrainingFingerprint:
    """Compute a deterministic training fingerprint."""
    cv = code_version or _git_sha()
    config_hash = _stable_json_hash(config)
    data_fp_hash = _stable_json_hash({
        "row_count": data_fingerprint.row_count,
        "column_hash": data_fingerprint.column_hash,
        "first_date": data_fingerprint.first_date,
        "last_date": data_fingerprint.last_date,
    })

    composite = _stable_json_hash({
        "code_version": cv,
        "config_hash": config_hash,
        "symbol": symbol.upper(),
        "timeframe": timeframe.upper(),
        "window_start": window_start,
        "window_end": window_end,
        "global_end": global_end,
        "data_fingerprint_hash": data_fp_hash,
    })

    return TrainingFingerprint(
        code_version=cv,
        config_hash=config_hash,
        symbol=symbol.upper(),
        timeframe=timeframe.upper(),
        window_start=window_start,
        window_end=window_end,
        global_end=global_end,
        data_fingerprint_hash=data_fp_hash,
        fingerprint_hash=composite,
    )


def check_checkpoint(
    current: TrainingFingerprint,
    stored_fingerprint_path: Optional[Path],
) -> CheckpointDecision:
    """Compare current fingerprint against stored.

    Returns SKIP_CHECKPOINT_HIT if they match, RETRAIN otherwise.
    """
    if stored_fingerprint_path is None or not stored_fingerprint_path.exists():
        return CheckpointDecision(
            action="RETRAIN",
            reason="no_stored_fingerprint",
            current_fingerprint=current.fingerprint_hash,
            stored_fingerprint=None,
        )

    try:
        stored = json.loads(stored_fingerprint_path.read_text(encoding="utf-8"))
        stored_hash = str(stored.get("fingerprint_hash", ""))
    except Exception:
        return CheckpointDecision(
            action="RETRAIN",
            reason="stored_fingerprint_unreadable",
            current_fingerprint=current.fingerprint_hash,
            stored_fingerprint=None,
        )

    if current.fingerprint_hash == stored_hash:
        return CheckpointDecision(
            action="SKIP_CHECKPOINT_HIT",
            reason="fingerprint_match",
            current_fingerprint=current.fingerprint_hash,
            stored_fingerprint=stored_hash,
        )

    return CheckpointDecision(
        action="RETRAIN",
        reason="fingerprint_mismatch",
        current_fingerprint=current.fingerprint_hash,
        stored_fingerprint=stored_hash,
    )


def save_fingerprint(fp: TrainingFingerprint, path: Path) -> None:
    """Persist fingerprint to JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "code_version": fp.code_version,
        "config_hash": fp.config_hash,
        "symbol": fp.symbol,
        "timeframe": fp.timeframe,
        "window_start": fp.window_start,
        "window_end": fp.window_end,
        "global_end": fp.global_end,
        "data_fingerprint_hash": fp.data_fingerprint_hash,
        "fingerprint_hash": fp.fingerprint_hash,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
