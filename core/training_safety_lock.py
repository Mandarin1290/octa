"""Training safety lock guards and manifest helpers.

Provides fail-closed assertions to prevent accidental training runs
unless a recent global gate run has been validated and explicitly ARMed.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


class TrainingSafetyLockError(Exception):
    pass


class GateManifestMissingError(TrainingSafetyLockError):
    pass


class GateArtifactsInvalidError(TrainingSafetyLockError):
    pass


class ConfigMismatchError(TrainingSafetyLockError):
    pass


class StrictCascadeDisabledError(TrainingSafetyLockError):
    pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_of_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _json_default(o: Any):
    """Best-effort JSON encoding for config fingerprinting.

    Must be stable across sweep (manifest writer) and runtime (safety lock),
    and handle common non-JSON-native types like Path and numpy scalars.
    """
    try:
        item = getattr(o, "item", None)
        if callable(item):
            return item()
    except Exception:
        pass

    try:
        if isinstance(o, Path):
            return str(o)
    except Exception:
        pass

    return str(o)


def load_latest_gate_run(manifest_path_or_dir: str | Path) -> Dict[str, Any]:
    if manifest_path_or_dir is None:
        dirp = Path("reports") / "gates"
    else:
        p = Path(manifest_path_or_dir)
        if p.is_file():
            try:
                return json.loads(p.read_text())
            except Exception as e:
                raise GateManifestMissingError(f"failed to read manifest {p}: {e}") from e
        if p.exists() and p.is_dir():
            dirp = p
        else:
            raise GateManifestMissingError(f"specified manifest path not found: {p}")

    if not dirp.exists():
        raise GateManifestMissingError(f"gate runs dir not found: {dirp}")

    manifests = sorted(dirp.rglob("gate_manifest.json"), key=lambda x: x.stat().st_mtime, reverse=True)
    if not manifests:
        raise GateManifestMissingError(f"no gate_manifest.json files found under {dirp}")
    m = manifests[0]
    try:
        return json.loads(m.read_text())
    except Exception as e:
        raise GateManifestMissingError(f"failed to parse manifest {m}: {e}") from e


def verify_gate_artifacts(manifest: Dict[str, Any], timeframe: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": False, "details": []}
    artifacts_dir = Path(manifest.get("artifacts_dir") or (Path(manifest.get("reports_dir" or "")) / "cascade" / manifest.get("run_id", "")))
    if not artifacts_dir.exists():
        out["details"].append(f"artifacts_dir_missing: {artifacts_dir}")
        raise GateArtifactsInvalidError("artifacts_dir missing")

    tf = timeframe
    pass_file = artifacts_dir / f"pass_symbols_{tf}.txt"
    fail_file = artifacts_dir / f"fail_symbols_{tf}.txt"
    err_file = artifacts_dir / f"err_symbols_{tf}.txt"
    missing = []
    for f in (pass_file, fail_file, err_file):
        if not f.exists():
            missing.append(str(f.name))
    if missing:
        out["details"].append(f"missing_files: {missing}")
        raise GateArtifactsInvalidError(f"missing gate artifact files: {missing}")

    out["ok"] = True
    return out


def verify_config_alignment(manifest: Dict[str, Any], train_config: Any) -> Dict[str, Any]:
    # Compute simple fingerprint of train_config if possible
    out = {"ok": False, "details": []}
    try:
        raw = train_config.model_dump() if hasattr(train_config, "model_dump") else (train_config.dict() if hasattr(train_config, "dict") else {})
        cfg_json = json.dumps(raw, sort_keys=True, default=_json_default)
        cfg_fp = _sha256_of_str(cfg_json)
    except Exception as e:
        out["details"].append("failed_compute_config_fp")
        raise ConfigMismatchError("could not compute train config fingerprint") from e

    manifest_fp = manifest.get("config_fingerprint")
    if not manifest_fp:
        out["details"].append("manifest_missing_config_fingerprint")
        raise ConfigMismatchError("manifest missing config_fingerprint")

    if str(manifest_fp) != str(cfg_fp):
        out["details"].append("config_fingerprint_mismatch")
        raise ConfigMismatchError("train config does not match gate config (fingerprint mismatch)")

    out["ok"] = True
    return out


def verify_strict_cascade_enabled(train_config: Any) -> Dict[str, Any]:
    out = {"ok": False, "details": []}
    try:
        raw = train_config.model_dump() if hasattr(train_config, "model_dump") else (train_config.dict() if hasattr(train_config, "dict") else {})
        # If there is an explicit flag disabling strict cascade, fail
        continue_on_fail = False
        # Check common places
        if isinstance(raw, dict):
            continue_on_fail = bool(raw.get("continue_on_fail", False) or raw.get("allow_continue_on_fail", False))
    except Exception as e:
        out["details"].append("failed_inspect_config")
        raise StrictCascadeDisabledError("could not verify strict cascade") from e

    if continue_on_fail:
        out["details"].append("continue_on_fail_enabled")
        raise StrictCascadeDisabledError("strict cascade disabled (continue_on_fail enabled)")

    out["ok"] = True
    return out


def emit_audit_log(event: Dict[str, Any], path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    entry = {"ts": _now_iso(), **event}
    with p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, default=str) + "\n")


def assert_training_armed(train_config: Any, symbol: str, timeframe: str, manifest_path_or_dir: Optional[str | Path] = None, max_age_days: int = 14) -> None:
    # load latest manifest
    manifest = load_latest_gate_run(manifest_path_or_dir or Path("reports") / "gates")
    # check ARMED.ok: prefer canonical assurance location, fall back to manifest_dir
    dataset = manifest.get("dataset") or manifest.get("asset_class") or "unknown"
    reports_dir = Path(manifest.get("reports_dir") or Path("reports"))
    canonical_dir = reports_dir / "assurance" / "armed" / str(dataset) / str(manifest.get("run_id", ""))
    canonical_armed = canonical_dir / "ARMED.ok"

    gate_dir = Path(manifest.get("manifest_dir") or (Path("reports") / "gates" / manifest.get("run_id", "")))
    legacy_armed = gate_dir / "ARMED.ok"

    if not canonical_armed.exists() and not legacy_armed.exists():
        raise GateArtifactsInvalidError(f"Gate run not armed (ARMED.ok missing). Checked: {canonical_armed} and {legacy_armed}")

    # check age
    try:
        created = manifest.get("created_utc")
        if created:
            # parse ISO naive
            d = datetime.fromisoformat(created.replace("Z", "+00:00"))
            delta = datetime.now(timezone.utc) - d
            if delta.days > int(max_age_days):
                raise GateArtifactsInvalidError(f"Gate run too old: {delta.days} days")
    except GateArtifactsInvalidError:
        raise
    except Exception as e:
        # conservative: fail-closed
        raise GateArtifactsInvalidError("failed to parse manifest created_utc") from e

    # verify strict cascade
    verify_strict_cascade_enabled(train_config)

    # verify config alignment
    verify_config_alignment(manifest, train_config)

    # verify artifacts for timeframe (and parent pass)
    verify_gate_artifacts(manifest, timeframe)

    # verify parent PASS exists if timeframe is not 1D
    if timeframe not in ("1D", "1DAY"):
        parent = "1D"
        artifacts_dir = Path(manifest.get("artifacts_dir") or (Path("reports") / "cascade" / manifest.get("run_id", "")))
        pass_file = artifacts_dir / f"pass_symbols_{parent}.txt"
        if not pass_file.exists():
            raise GateArtifactsInvalidError(f"parent pass file missing: {pass_file}")
        base = symbol.upper().split("_")[0]
        content = [s.strip().upper() for s in pass_file.read_text().splitlines() if s.strip()]
        if base not in content and symbol.upper() not in content:
            raise GateArtifactsInvalidError(f"parent {parent} did not PASS for symbol {symbol}")

    # if all checks pass, return None
    return None
