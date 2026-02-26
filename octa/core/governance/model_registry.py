from __future__ import annotations

import hashlib
import json
import os
import platform
from datetime import datetime, timezone
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

from .immutability_guard import assert_write_allowed


def canonical_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def compute_model_id(symbol: str, timeframe: str, artifact_sha256: str, config_hash: str, length: int = 16) -> str:
    raw = f"{str(symbol)}|{str(timeframe)}|{str(artifact_sha256)}|{str(config_hash)}"
    return sha256_bytes(raw.encode("utf-8"))[: int(length)]


def _default_deps_provider() -> Iterable[str]:
    rows = []
    for dist in importlib_metadata.distributions():
        name = None
        version = None
        try:
            name = dist.metadata.get("Name")
        except Exception:
            name = None
        try:
            version = dist.version
        except Exception:
            version = None
        if not name:
            continue
        rows.append(f"{str(name).strip()}=={str(version).strip() if version is not None else ''}")
    return rows


def compute_deps_fingerprint(provider: Optional[Callable[[], Iterable[str]]] = None) -> str:
    src = provider or _default_deps_provider
    lines = [str(x).strip() for x in src() if str(x).strip()]
    if not lines:
        raise RuntimeError("deps_fingerprint_unavailable")
    lines = sorted(set(lines))
    payload = "\n".join(lines)
    return sha256_bytes(payload.encode("utf-8"))


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _compute_entry_sha(entry_without_hash: Mapping[str, Any]) -> str:
    payload = canonical_dumps(dict(entry_without_hash))
    return sha256_bytes(payload.encode("utf-8"))


def validate_entry(entry: Mapping[str, Any]) -> bool:
    required_top = [
        "schema_version",
        "model_id",
        "created_at",
        "stage",
        "symbol",
        "timeframe",
        "artifact",
        "training",
        "environment",
        "gates",
        "promotion",
        "evidence",
        "entry_sha256",
    ]
    for key in required_top:
        if key not in entry:
            return False
    artifact_raw = entry.get("artifact")
    training_raw = entry.get("training")
    environment_raw = entry.get("environment")
    gates_raw = entry.get("gates")
    promotion_raw = entry.get("promotion")
    evidence_raw = entry.get("evidence")
    if (
        not isinstance(artifact_raw, Mapping)
        or not isinstance(training_raw, Mapping)
        or not isinstance(environment_raw, Mapping)
        or not isinstance(gates_raw, Mapping)
        or not isinstance(promotion_raw, Mapping)
        or not isinstance(evidence_raw, Mapping)
    ):
        return False
    artifact: Mapping[str, Any] = artifact_raw
    training: Mapping[str, Any] = training_raw
    environment: Mapping[str, Any] = environment_raw
    gates: Mapping[str, Any] = gates_raw
    promotion: Mapping[str, Any] = promotion_raw
    evidence: Mapping[str, Any] = evidence_raw
    for key in ["path", "sha256", "size_bytes"]:
        if key not in artifact:
            return False
    for key in ["feature_code_hash", "config_hash"]:
        if key not in training:
            return False
    for key in ["python", "platform", "deps_fingerprint"]:
        if key not in environment:
            return False
    for key in ["structural", "risk", "performance", "drift"]:
        if key not in gates:
            return False
    for key in ["status", "reason"]:
        if key not in promotion:
            return False
    for key in ["evidence_dir", "run_id", "inputs_hash", "outputs_hash"]:
        if key not in evidence:
            return False

    raw = dict(entry)
    expected = str(raw.pop("entry_sha256", ""))
    actual = _compute_entry_sha(raw)
    return bool(expected) and expected == actual


def _append_jsonl_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def append_entry(
    ctx: Mapping[str, Any],
    entry: Mapping[str, Any],
    registry_path: str | Path = Path("octa") / "var" / "registry" / "models" / "registry.jsonl",
    evidence_dir: str | Path = Path("octa") / "var" / "evidence" / "registry_append",
) -> bool:
    path = Path(registry_path)
    ev_dir = Path(evidence_dir)
    ev_dir.mkdir(parents=True, exist_ok=True)

    mode = str(ctx.get("mode", "")).strip().lower()
    try:
        assert_write_allowed(
            ctx,
            operation="registry_write",
            target=str(path),
            details={"subsystem": "model_registry_append", "stage": str(ctx.get("stage", ""))},
        )
    except RuntimeError:
        if mode == "shadow":
            payload = {
                "ts_utc": _now_iso_utc(),
                "reason": "IMMUTABLE_PROD_BLOCK",
                "mode": mode,
                "operation": "registry_write",
                "target": str(path),
                "action": "shadow_warn_skip",
                "entry_preview": {"model_id": entry.get("model_id"), "symbol": entry.get("symbol"), "timeframe": entry.get("timeframe")},
            }
            (ev_dir / "registry_append.json").write_text(
                json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
            return False
        raise

    stage = str(ctx.get("stage", "")).strip().lower()
    if stage != "research":
        payload = {
            "ts_utc": _now_iso_utc(),
            "reason": "registry_append_skipped_non_research_stage",
            "mode": mode,
            "stage": stage,
            "operation": "registry_write",
            "target": str(path),
        }
        (ev_dir / "registry_append.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        return False

    payload = dict(entry)
    payload_no_hash = dict(payload)
    payload_no_hash.pop("entry_sha256", None)
    payload["entry_sha256"] = _compute_entry_sha(payload_no_hash)
    if not validate_entry(payload):
        raise RuntimeError("model_registry_entry_invalid")

    line = canonical_dumps(payload)
    _append_jsonl_line(path, line)
    (ev_dir / "registry_append.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    return True


def get_latest(
    symbol: str,
    timeframe: str,
    registry_path: str | Path = Path("octa") / "var" / "registry" / "models" / "registry.jsonl",
) -> Optional[Dict[str, Any]]:
    path = Path(registry_path)
    if not path.exists():
        return None
    target_symbol = str(symbol)
    target_tf = str(timeframe)
    best: Optional[Dict[str, Any]] = None
    best_key: tuple[str, str] = ("", "")
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            raw = str(line).strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            if str(obj.get("symbol")) != target_symbol or str(obj.get("timeframe")) != target_tf:
                continue
            if not validate_entry(obj):
                continue
            key = (str(obj.get("created_at", "")), str(obj.get("model_id", "")))
            if best is None or key > best_key:
                best = obj
                best_key = key
    return best


def build_registry_entry(
    *,
    symbol: str,
    timeframe: str,
    artifact_path: str,
    artifact_sha256: str,
    artifact_size_bytes: int,
    feature_code_hash: str,
    config_hash: str,
    stage: str,
    run_id: str,
    evidence_dir: str,
    inputs_hash: str,
    outputs_hash: str,
    gates: Mapping[str, str],
    promotion_status: str,
    promotion_reason: str,
    deps_fingerprint: str,
    created_at: Optional[str] = None,
    asset_class: Optional[str] = None,
    training_data_hash: Optional[str] = None,
    hyperparam_hash: Optional[str] = None,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    model_id = compute_model_id(symbol, timeframe, artifact_sha256, config_hash)
    entry: Dict[str, Any] = {
        "schema_version": 1,
        "model_id": model_id,
        "created_at": str(created_at or _now_iso_utc()),
        "stage": str(stage),
        "symbol": str(symbol),
        "timeframe": str(timeframe),
        "asset_class": asset_class,
        "artifact": {
            "path": str(artifact_path),
            "sha256": str(artifact_sha256),
            "size_bytes": int(artifact_size_bytes),
        },
        "training": {
            "training_data_hash": training_data_hash,
            "feature_code_hash": str(feature_code_hash),
            "config_hash": str(config_hash),
            "hyperparam_hash": hyperparam_hash,
            "seed": seed,
        },
        "environment": {
            "python": str(platform.python_version()),
            "platform": str(platform.platform()),
            "deps_fingerprint": str(deps_fingerprint),
        },
        "gates": {
            "structural": str(gates.get("structural", "HOLD")),
            "risk": str(gates.get("risk", "HOLD")),
            "performance": str(gates.get("performance", "HOLD")),
            "drift": str(gates.get("drift", "HOLD")),
        },
        "promotion": {
            "status": str(promotion_status),
            "reason": str(promotion_reason),
        },
        "evidence": {
            "evidence_dir": str(evidence_dir),
            "run_id": str(run_id),
            "inputs_hash": str(inputs_hash),
            "outputs_hash": str(outputs_hash),
        },
    }
    entry["entry_sha256"] = _compute_entry_sha(entry)
    return entry
