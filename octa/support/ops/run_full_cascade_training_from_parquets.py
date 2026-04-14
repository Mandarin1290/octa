from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import queue
import re
import signal
import subprocess
import sys
import time
import traceback
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from octa import __version__ as OCTA_VERSION
from octa.core.cascade.policies import DEFAULT_TIMEFRAMES
from octa_training.core.institutional_gates import evaluate_cross_timeframe_consistency
from octa_training.core.config import load_config
from octa_training.core.training_policy import (
    prototype_allowed_asset_classes,
    resolve_active_prototype_policy,
)
from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training
from octa.core.data.sources.altdata.orchestrator import load_altdat_config
from octa.core.data.sources.altdata.cache import resolve_cache_root
from octa.support.ops.universe_preflight import ASSET_CLASS_ALIASES, KNOWN_ASSET_CLASSES
from octa.core.governance.governance_audit import GovernanceAudit, EVENT_TRAINING_RUN


MAX_EXCEPTION_MESSAGE_CHARS = 2000
MAX_EXCEPTION_TRACEBACK_CHARS = 20000
VALID_SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9._-]*$")


class RunAbortRequested(Exception):
    """Raised when the operator terminates a run and we must fail closed."""


class SymbolTrainingTimeout(Exception):
    """Raised when a symbol-level training process exceeds its bounded runtime."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_id() -> str:
    return f"full_cascade_{_utc_now().strftime('%Y%m%dT%H%M%SZ')}"


def _sha256_bytes(data: bytes) -> str:
    import hashlib

    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _sha256_file(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _append_jsonl(path: Path, row: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
        fh.write("\n")


def _log(path: Path, msg: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(msg.rstrip() + "\n")
    print(msg)


def _git_hash() -> Optional[str]:
    try:
        res = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True)
        return res.stdout.strip() or None
    except Exception:
        return None


def _python_version() -> str:
    res = subprocess.run([sys.executable, "-V"], capture_output=True, text=True)
    return (res.stdout or res.stderr).strip()


def _discover_preflight_files(preflight_dir: Path) -> Dict[str, Path]:
    trainable = list(preflight_dir.rglob("trainable_symbols.txt"))
    inventory = list(preflight_dir.rglob("inventory.jsonl"))
    summary = list(preflight_dir.rglob("summary.json"))
    if len(trainable) != 1 or len(inventory) != 1:
        raise RuntimeError("Preflight outputs not found or ambiguous")
    out = {
        "trainable": trainable[0],
        "inventory": inventory[0],
    }
    if len(summary) == 1:
        out["summary"] = summary[0]
    return out


def _load_trainable_symbols(path: Path) -> List[str]:
    symbols = [line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return sorted(dict.fromkeys(symbols))


def _load_inventory(path: Path) -> Dict[str, Dict[str, Any]]:
    inventory: Dict[str, Dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        sym = str(raw.get("symbol", "")).upper()
        tfs = raw.get("tfs") or {}
        asset_class = str(raw.get("asset_class", "unknown")).strip().lower() or "unknown"
        if sym:
            inventory[sym] = {
                "asset_class": asset_class,
                "tfs": {str(tf).upper(): list(paths) for tf, paths in tfs.items()},
            }
    return inventory


def _pick_rep(paths: Sequence[str]) -> Optional[str]:
    if not paths:
        return None
    return sorted(paths, key=lambda p: (len(p), p))[0]


def _build_parquet_paths(symbol: str, inventory: Dict[str, Dict[str, Any]], tfs: Sequence[str] = DEFAULT_TIMEFRAMES) -> Dict[str, str]:
    by_tf = (inventory.get(symbol) or {}).get("tfs", {})
    out: Dict[str, str] = {}
    for tf in tfs:
        rep = _pick_rep(by_tf.get(tf, []))
        if rep:
            out[tf] = rep
    return out


def _normalize_asset_class_filter(values: Optional[Sequence[str]]) -> Optional[Tuple[str, ...]]:
    if not values:
        return None
    normalized: List[str] = []
    seen: set[str] = set()
    for raw in values:
        label = str(raw).strip().lower()
        if not label:
            continue
        canonical = ASSET_CLASS_ALIASES.get(label, label)
        if canonical not in KNOWN_ASSET_CLASSES and canonical != "unknown":
            canonical = label
        if canonical in seen:
            continue
        seen.add(canonical)
        normalized.append(canonical)
    return tuple(sorted(normalized)) or None


def _metrics_summary(metrics: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(metrics, dict):
        return {}
    keys = ["n_trades", "sharpe", "sortino", "max_drawdown", "cagr", "profit_factor"]
    out: Dict[str, Any] = {}
    for k in keys:
        if k in metrics:
            v = metrics.get(k)
            if isinstance(v, float):
                out[k] = round(v, 10)
            else:
                out[k] = v
    return out


def _metrics_valid(metrics: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    if not isinstance(metrics, dict):
        return False, "metrics_missing"
    n_trades = metrics.get("n_trades")
    if n_trades is None:
        return False, "metrics_missing_n_trades"
    try:
        if int(n_trades) <= 0:
            return False, "metrics_no_trades"
    except Exception:
        return False, "metrics_invalid_n_trades"
    return True, ""


def _artifacts_valid(paths: Optional[Sequence[str]]) -> Tuple[bool, str]:
    if not paths:
        return False, "missing_model_artifacts"
    missing = [p for p in paths if not Path(p).exists()]
    if missing:
        return False, "missing_model_artifacts"
    return True, ""


def _validate_tradeable_artifacts(
    paths: Optional[Sequence[str]],
    *,
    expected_symbol: str,
    expected_asset_class: str,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "valid": False,
        "reason": "missing_model_artifacts",
        "checked_pkls": [],
        "valid_tradeable_pkls": [],
        "invalid_details": [],
    }
    ok_paths, why_paths = _artifacts_valid(paths)
    if not ok_paths:
        result["reason"] = why_paths
        return result

    from octa_training.core.artifact_io import load_tradeable_artifact, read_meta

    pkl_candidates = [
        str(Path(p))
        for p in (paths or [])
        if str(p).lower().endswith(".pkl") and Path(p).name != "model.pkl"
    ]
    result["checked_pkls"] = list(pkl_candidates)
    if not pkl_candidates:
        result["reason"] = "missing_tradeable_artifact_bundle"
        return result

    for pkl_path in pkl_candidates:
        p = Path(pkl_path)
        meta_path = p.with_suffix(".meta.json")
        sha_path = p.with_suffix(".sha256")
        detail: Dict[str, Any] = {
            "pkl_path": str(p),
            "meta_path": str(meta_path),
            "sha_path": str(sha_path),
            "valid": False,
        }
        if not meta_path.exists() or not sha_path.exists():
            detail["reason"] = "artifact_sidecars_missing"
            result["invalid_details"].append(detail)
            continue
        try:
            artifact = load_tradeable_artifact(str(p), str(sha_path))
            meta = read_meta(str(meta_path))
        except Exception as exc:
            detail["reason"] = f"artifact_load_failed:{type(exc).__name__}"
            result["invalid_details"].append(detail)
            continue

        artifact_kind = str(artifact.get("artifact_kind", "")).strip().lower()
        meta_kind = str(getattr(meta, "artifact_kind", "")).strip().lower()
        artifact_symbol = str((artifact.get("asset", {}) or {}).get("symbol", "")).strip().upper()
        artifact_asset_class = str((artifact.get("asset", {}) or {}).get("asset_class", "")).strip().lower()
        artifact_asset_class = ASSET_CLASS_ALIASES.get(artifact_asset_class, artifact_asset_class)
        expected_asset_class_norm = ASSET_CLASS_ALIASES.get(expected_asset_class, expected_asset_class)

        if artifact_kind != "tradeable" or meta_kind != "tradeable":
            detail["reason"] = "artifact_kind_not_tradeable"
            detail["artifact_kind"] = artifact_kind
            detail["meta_artifact_kind"] = meta_kind
            result["invalid_details"].append(detail)
            continue
        if artifact_symbol != str(expected_symbol).upper():
            detail["reason"] = "artifact_symbol_mismatch"
            detail["artifact_symbol"] = artifact_symbol
            result["invalid_details"].append(detail)
            continue
        if expected_asset_class_norm not in {"", "unknown"} and artifact_asset_class not in {"", expected_asset_class_norm}:
            detail["reason"] = "artifact_asset_class_mismatch"
            detail["artifact_asset_class"] = artifact_asset_class
            result["invalid_details"].append(detail)
            continue
        detail["valid"] = True
        detail["reason"] = "tradeable_artifact_valid"
        result["valid_tradeable_pkls"].append(str(p))

    if result["valid_tradeable_pkls"]:
        result["valid"] = True
        result["reason"] = "tradeable_artifact_valid"
        return result

    if len(result["invalid_details"]) == 1:
        detail_reason = str((result["invalid_details"][0] or {}).get("reason") or "").strip()
        if detail_reason:
            result["reason"] = detail_reason
            return result

    result["reason"] = "invalid_tradeable_artifact"
    return result


def _build_symbol_request_report(
    requested_symbols: Sequence[str],
    *,
    trainable_symbols: Sequence[str],
    inventory: Dict[str, Dict[str, Any]],
    allowed_asset_classes: Optional[Sequence[str]] = None,
) -> Tuple[List[str], Dict[str, Any], List[Dict[str, Any]]]:
    accepted: List[str] = []
    rejected: List[Dict[str, Any]] = []
    duplicates_removed: List[str] = []
    invalid_format: List[str] = []
    missing_symbols: List[str] = []
    seen: set[str] = set()
    trainable_set = {str(sym).upper() for sym in trainable_symbols}
    inventory_set = {str(sym).upper() for sym in inventory.keys()}
    allowed_asset_class_set = {str(v).strip().lower() for v in (allowed_asset_classes or []) if str(v).strip()}

    for raw in requested_symbols:
        sym = str(raw or "").strip().upper()
        if not sym:
            invalid_format.append(sym)
            rejected.append({"symbol": sym, "reason": "empty_requested_symbol"})
            continue
        if not VALID_SYMBOL_RE.match(sym):
            invalid_format.append(sym)
            rejected.append({"symbol": sym, "reason": "invalid_requested_symbol_format"})
            continue
        if sym in seen:
            duplicates_removed.append(sym)
            continue
        seen.add(sym)
        if sym not in trainable_set or sym not in inventory_set:
            missing_symbols.append(sym)
            rejected.append(
                {
                    "symbol": sym,
                    "reason": "symbol_not_trainable_or_missing",
                    "detail": {"trainable": sym in trainable_set, "in_inventory": sym in inventory_set},
                }
            )
            continue
        if allowed_asset_class_set:
            sym_asset_class = str((inventory.get(sym) or {}).get("asset_class", "unknown")).strip().lower()
            if sym_asset_class not in allowed_asset_class_set:
                rejected.append(
                    {
                        "symbol": sym,
                        "reason": "symbol_excluded_by_training_policy",
                        "detail": {"asset_class": sym_asset_class, "allowed_asset_classes": sorted(allowed_asset_class_set)},
                    }
                )
                continue
        accepted.append(sym)

    report = {
        "requested_count": len(list(requested_symbols)),
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "duplicates_removed_count": len(duplicates_removed),
        "invalid_format_count": len(invalid_format),
        "missing_count": len(missing_symbols),
        "accepted_symbols": list(accepted),
        "duplicates_removed": sorted(dict.fromkeys(duplicates_removed)),
        "invalid_format_symbols": sorted(dict.fromkeys(invalid_format)),
        "rejected_symbols": rejected,
    }
    return accepted, report, rejected


def _classify_symbol_outcome(result: Dict[str, Any]) -> str:
    status = str(result.get("status", "")).upper()
    reason = str(result.get("reason", "") or "")
    stages = result.get("stages") or []
    if status == "DRY_RUN":
        return "dry_run"
    if status == "PASS":
        return "trained_successfully"
    if not stages:
        if reason in {"missing_parquet_paths", "symbol_not_trainable_or_missing"}:
            return "skipped"
        return "failed"
    if all(str(stage.get("status", "")).upper() == "SKIP" for stage in stages):
        return "skipped"
    if any(str(stage.get("status", "")).upper() == "TRAIN_ERROR" for stage in stages):
        return "failed"
    if any(
        stage.get("model_artifacts")
        and str((stage.get("artifact_validation") or {}).get("valid", True)).lower() == "false"
        for stage in stages
    ):
        return "trained_but_invalid"
    if status == "PASS":
        return "trained_successfully"
    if reason in {"missing_parquet_paths", "symbol_not_trainable_or_missing"}:
        return "skipped"
    return "failed"


def _compute_exit_code(*, error_reason: Optional[str], outcome_counts: Dict[str, int], hard_blockers: Sequence[str]) -> int:
    if error_reason or hard_blockers:
        return 2
    if int(outcome_counts.get("failed", 0)) > 0 or int(outcome_counts.get("trained_but_invalid", 0)) > 0:
        return 1
    return 0


def _compact_gate_result(gate: Any) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    if not isinstance(gate, dict):
        return {}, None
    return _build_gate_summary(gate)


def _truncate_text(value: Any, max_chars: int) -> str:
    text = str(value or "")
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3] + "..."


def _safe_json_obj(value: Any, *, max_depth: int = 6, max_items: int = 200, max_text: int = 4000) -> Any:
    if max_depth <= 0:
        return "<max_depth_exceeded>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _truncate_text(value, max_text)
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for i, (k, v) in enumerate(value.items()):
            if i >= max_items:
                out["<truncated_items>"] = f"{len(value) - max_items} more"
                break
            out[str(k)] = _safe_json_obj(v, max_depth=max_depth - 1, max_items=max_items, max_text=max_text)
        return out
    if isinstance(value, (list, tuple, set)):
        seq = list(value)
        out_list: List[Any] = []
        for i, v in enumerate(seq):
            if i >= max_items:
                out_list.append(f"<truncated_items:{len(seq) - max_items}>")
                break
            out_list.append(_safe_json_obj(v, max_depth=max_depth - 1, max_items=max_items, max_text=max_text))
        return out_list
    return _truncate_text(repr(value), max_text)


def _build_gate_summary(gate: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    gate_sanitized = _safe_json_obj(gate)
    summary: Dict[str, Any] = {}

    passed = gate.get("passed")
    if passed is not None:
        summary["passed"] = bool(passed)
    if "reason" in gate:
        summary["reason"] = gate.get("reason")

    reasons = gate.get("reasons")
    if isinstance(reasons, list):
        summary["reasons"] = [_truncate_text(r, 300) for r in reasons[:50]]

    failed_checks: List[Dict[str, Any]] = []
    raw_thresholds = gate.get("thresholds") if isinstance(gate.get("thresholds"), dict) else {}

    raw_failed_checks = gate.get("failed_checks")
    if isinstance(raw_failed_checks, list):
        for item in raw_failed_checks:
            if isinstance(item, dict):
                failed_checks.append(
                    {
                        "metric": item.get("metric") or item.get("name"),
                        "value": item.get("value"),
                        "threshold": item.get("threshold"),
                        "comparator": item.get("comparator") or item.get("op"),
                        "reason": item.get("reason"),
                    }
                )
            else:
                failed_checks.append(
                    {
                        "metric": str(item),
                        "value": None,
                        "threshold": raw_thresholds.get(str(item)),
                        "comparator": None,
                        "reason": None,
                    }
                )

    effective_thresholds: Dict[str, Dict[str, Any]] = {}
    diagnostics = gate.get("diagnostics")
    if isinstance(diagnostics, list):
        for d in diagnostics:
            if not isinstance(d, dict):
                continue
            name = str(d.get("name", "")).strip()
            op = d.get("op")
            threshold = d.get("threshold")
            passed_flag = bool(d.get("passed"))
            if name:
                effective_thresholds[name] = {"threshold": threshold, "comparator": op}
            if not passed_flag:
                failed_checks.append(
                    {
                        "metric": name or None,
                        "value": d.get("value"),
                        "threshold": threshold,
                        "comparator": op,
                        "reason": d.get("reason"),
                    }
                )
            if name == "metric_scale_info":
                summary["metric_scale_info"] = d.get("reason")
            if name == "net_of_cost":
                summary["net_of_cost"] = bool(d.get("value") in {1, 1.0, True})

    try:
        rob = gate.get("robustness") if isinstance(gate, dict) else None
        if isinstance(rob, dict):
            details = rob.get("details") or {}
            mc = details.get("monte_carlo")
            if isinstance(mc, dict):
                summary["monte_carlo"] = _safe_json_obj(mc)
            wf = details.get("walk_forward")
            if isinstance(wf, dict):
                summary["walk_forward"] = _safe_json_obj(wf)
            rg = details.get("regime_stability")
            if isinstance(rg, dict):
                summary["regime_stability"] = _safe_json_obj(rg)
            cs = details.get("cost_stress")
            if isinstance(cs, dict):
                summary["cost_stress"] = _safe_json_obj(cs)
            liq = details.get("liquidity")
            if isinstance(liq, dict):
                summary["liquidity"] = _safe_json_obj(liq)
    except Exception:
        pass

    deduped: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for fc in failed_checks:
        key = json.dumps(_safe_json_obj(fc), sort_keys=True, ensure_ascii=False)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(fc)
    summary["failed_checks"] = deduped

    thresholds = gate.get("thresholds")
    if isinstance(thresholds, dict):
        summary["thresholds"] = _safe_json_obj(thresholds)
    elif effective_thresholds:
        summary["effective_thresholds"] = _safe_json_obj(effective_thresholds)

    explained = bool(deduped) or bool(summary.get("reasons"))
    if summary.get("passed") is False and not explained:
        summary["unexplained"] = True
        summary["raw_gate_dump"] = gate_sanitized

    raw_gate: Optional[Dict[str, Any]] = None
    try:
        raw = json.dumps(gate_sanitized, ensure_ascii=False, sort_keys=True)
        if len(raw) <= 200000:
            raw_gate = gate_sanitized if isinstance(gate_sanitized, dict) else None
    except Exception:
        raw_gate = None
    return summary, raw_gate


def _write_train_exception_artifacts(
    evidence_dir: Path,
    *,
    symbol: str,
    timeframe: str,
    exception_type: str,
    message: str,
    traceback_full: str,
    where: str,
) -> Dict[str, str]:
    base = evidence_dir / "exceptions" / symbol / timeframe
    base.mkdir(parents=True, exist_ok=True)
    exc_json = base / "exception.json"
    tb_txt = base / "traceback.txt"
    payload = {
        "exception_type": exception_type,
        "message": _truncate_text(message, MAX_EXCEPTION_MESSAGE_CHARS),
        "traceback": _truncate_text(traceback_full, MAX_EXCEPTION_TRACEBACK_CHARS),
        "where": where,
        "symbol": symbol,
        "timeframe": timeframe,
    }
    _write_json(exc_json, payload)
    tb_txt.write_text(traceback_full, encoding="utf-8")
    return {
        "exception_json": str(exc_json.relative_to(evidence_dir)),
        "traceback_txt": str(tb_txt.relative_to(evidence_dir)),
    }


def _write_unexplained_gate_artifact(
    evidence_dir: Path,
    *,
    symbol: str,
    timeframe: str,
    gate_summary: Dict[str, Any],
    gate_raw: Optional[Dict[str, Any]],
) -> str:
    base = evidence_dir / "gate_unexplained"
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"{symbol}_{timeframe}.json"
    payload = {
        "reason": "gate_failed_unexplained",
        "symbol": symbol,
        "timeframe": timeframe,
        "gate_summary": _safe_json_obj(gate_summary),
        "raw_gate_dump": _safe_json_obj(gate_raw),
        "timestamp_utc": _utc_now_iso(),
    }
    _write_json(path, payload)
    return str(path.relative_to(evidence_dir))


def _normalize_decisions(
    decisions: List[Any],
    metrics_by_tf: Dict[str, Any],
    expected_symbol: str,
    default_asset_class: str = "unknown",
    cascade_tfs: Sequence[str] = DEFAULT_TIMEFRAMES,
) -> Tuple[List[Dict[str, Any]], bool, Optional[str], Optional[Dict[str, Any]]]:
    out: List[Dict[str, Any]] = []
    prev_pass = True
    overall_pass = True
    top_fail_reason: Optional[str] = None
    top_detail: Optional[Dict[str, Any]] = None
    for tf in cascade_tfs:
        match = next((d for d in decisions if str(getattr(d, "timeframe", "")).upper() == tf), None)
        status = "SKIP"
        reason = "missing_decision"
        decision_detail: Dict[str, Any] = {}
        if match is not None:
            status = str(getattr(match, "status", "")).upper()
            reason = getattr(match, "reason", None)
            raw_detail = getattr(match, "details", None)
            if isinstance(raw_detail, dict):
                decision_detail = _safe_json_obj(raw_detail)
        if status == "FAIL":
            # Backward-compatible normalization for older decision producers.
            if str(reason or "").lower() in {"train_error", "train_exception"}:
                status = "TRAIN_ERROR"
            else:
                status = "GATE_FAIL"
        metrics = None
        model_artifacts = None
        features_used = None
        altdata_sources = None
        altdata_enabled = False
        altdata_meta = None
        training_window = None
        gate_summary: Dict[str, Any] = {}
        gate_result: Optional[Dict[str, Any]] = None
        monte_carlo = None
        walk_forward = None
        regime_stability = None
        cost_stress = None
        liquidity = None
        leakage_audit = None
        artifact_validation: Dict[str, Any] = {"valid": False, "reason": "missing_model_artifacts", "checked_pkls": [], "valid_tradeable_pkls": [], "invalid_details": []}
        stage_asset_class = str(default_asset_class or "unknown").strip().lower() or "unknown"
        stage_asset_profile = None
        stage_asset_profile_kind = None
        stage_asset_profile_hash = None
        stage_asset_profile_source = None
        stage_asset_profile_legacy_fallback = None
        stage_training_policy = None
        stage_training_policy_source = None
        if tf in metrics_by_tf:
            metrics = (metrics_by_tf.get(tf) or {}).get("metrics")
            model_artifacts = (metrics_by_tf.get(tf) or {}).get("model_artifacts")
            features_used = (metrics_by_tf.get(tf) or {}).get("features_used")
            altdata_sources = (metrics_by_tf.get(tf) or {}).get("altdata_sources_used")
            altdata_enabled = bool((metrics_by_tf.get(tf) or {}).get("altdata_enabled"))
            altdata_meta = (metrics_by_tf.get(tf) or {}).get("altdata_meta")
            training_window = (metrics_by_tf.get(tf) or {}).get("training_window")
            gate_summary, gate_result = _compact_gate_result((metrics_by_tf.get(tf) or {}).get("gate"))
            monte_carlo = (metrics_by_tf.get(tf) or {}).get("monte_carlo")
            walk_forward = (metrics_by_tf.get(tf) or {}).get("walk_forward")
            regime_stability = (metrics_by_tf.get(tf) or {}).get("regime_stability")
            cost_stress = (metrics_by_tf.get(tf) or {}).get("cost_stress")
            liquidity = (metrics_by_tf.get(tf) or {}).get("liquidity")
            leakage_audit = (metrics_by_tf.get(tf) or {}).get("leakage_audit")
            stage_asset_class = str((metrics_by_tf.get(tf) or {}).get("asset_class", "unknown")).strip().lower() or "unknown"
            stage_asset_profile = (metrics_by_tf.get(tf) or {}).get("asset_profile")
            stage_asset_profile_kind = (metrics_by_tf.get(tf) or {}).get("asset_profile_kind")
            stage_asset_profile_hash = (metrics_by_tf.get(tf) or {}).get("asset_profile_hash")
            stage_asset_profile_source = (metrics_by_tf.get(tf) or {}).get("asset_profile_source")
            stage_asset_profile_legacy_fallback = (metrics_by_tf.get(tf) or {}).get("asset_profile_legacy_fallback")
            stage_training_policy = (metrics_by_tf.get(tf) or {}).get("training_policy")
            stage_training_policy_source = (metrics_by_tf.get(tf) or {}).get("training_policy_source")
            artifact_validation = _validate_tradeable_artifacts(
                model_artifacts,
                expected_symbol=expected_symbol,
                expected_asset_class=stage_asset_class,
            )
        if status == "PASS":
            ok_metrics, why_metrics = _metrics_valid(metrics)
            if not ok_metrics:
                status = "GATE_FAIL"
                reason = why_metrics
            elif not bool(artifact_validation.get("valid", False)):
                status = "GATE_FAIL"
                reason = str(artifact_validation.get("reason") or "invalid_tradeable_artifact")
            elif not isinstance(monte_carlo, dict):
                status = "GATE_FAIL"
                reason = "monte_carlo_missing"
            elif not bool(monte_carlo.get("passed", False)):
                status = "GATE_FAIL"
                reason = "monte_carlo_failed"
            elif not isinstance(walk_forward, dict):
                status = "GATE_FAIL"
                reason = "walkforward_missing"
            elif not bool(walk_forward.get("passed", False)):
                status = "GATE_FAIL"
                reason = "walkforward_failed"
            elif not isinstance(regime_stability, dict):
                status = "GATE_FAIL"
                reason = "regime_stability_missing"
            elif not bool(regime_stability.get("passed", False)):
                status = "GATE_FAIL"
                reason = "regime_stability_failed"
            elif not isinstance(cost_stress, dict):
                status = "GATE_FAIL"
                reason = "cost_stress_missing"
            elif not bool(cost_stress.get("passed", False)):
                status = "GATE_FAIL"
                reason = "cost_stress_failed"
            elif not isinstance(liquidity, dict):
                status = "GATE_FAIL"
                reason = "liquidity_missing"
            elif not bool(liquidity.get("passed", False)):
                status = "GATE_FAIL"
                reason = "liquidity_failed"
        elif status == "GATE_FAIL" and reason == "gate_failed":
            ok_metrics, _ = _metrics_valid(metrics)
            if not ok_metrics:
                reason = "invalid_metrics"
            elif gate_summary.get("passed") is False and bool(gate_summary.get("unexplained")):
                reason = "gate_failed_unexplained"
                gate_summary["reason"] = "gate_failed_unexplained"
        if not prev_pass:
            status = "SKIP"
            reason = "cascade_previous_not_pass"
        prev_pass = status == "PASS"
        if status != "PASS":
            overall_pass = False
            if top_fail_reason is None and status in {"GATE_FAIL", "TRAIN_ERROR"}:
                top_fail_reason = f"{reason}_{tf}" if reason else f"fail_{tf}"
                if reason in {"gate_failed", "gate_failed_unexplained"}:
                    top_detail = {"timeframe": tf, "gate_summary": gate_summary}
                elif status == "TRAIN_ERROR" or reason in {"train_error", "train_exception"}:
                    top_detail = {"timeframe": tf}
        out.append(
            {
                "timeframe": tf,
                "asset_class": stage_asset_class,
                "asset_profile": stage_asset_profile,
                "asset_profile_kind": stage_asset_profile_kind,
                "asset_profile_hash": stage_asset_profile_hash,
                "asset_profile_source": stage_asset_profile_source,
                "asset_profile_legacy_fallback": stage_asset_profile_legacy_fallback,
                "training_policy": stage_training_policy,
                "training_policy_source": stage_training_policy_source,
                "status": status,
                "reason": reason,
                "metrics_summary": _metrics_summary(metrics),
                "model_artifacts": list(model_artifacts or []),
                "features_used": list(features_used or []),
                "altdata_sources_used": list(altdata_sources or []),
                "altdata_enabled": bool(altdata_enabled),
                "altdata_meta": _safe_json_obj(altdata_meta) if isinstance(altdata_meta, dict) else altdata_meta,
                "training_window": training_window,
                "gate_summary": gate_summary,
                "gate_result": gate_result,
                "monte_carlo": _safe_json_obj(monte_carlo) if isinstance(monte_carlo, dict) else monte_carlo,
                "walk_forward": _safe_json_obj(walk_forward) if isinstance(walk_forward, dict) else walk_forward,
                "regime_stability": _safe_json_obj(regime_stability) if isinstance(regime_stability, dict) else regime_stability,
                "cost_stress": _safe_json_obj(cost_stress) if isinstance(cost_stress, dict) else cost_stress,
                "liquidity": _safe_json_obj(liquidity) if isinstance(liquidity, dict) else liquidity,
                "leakage_audit": _safe_json_obj(leakage_audit) if isinstance(leakage_audit, dict) else leakage_audit,
                "artifact_validation": _safe_json_obj(artifact_validation),
                "decision_detail": decision_detail,
            }
        )
    return out, overall_pass, top_fail_reason, top_detail


def _run_preflight(root: Path, preflight_out: Path, log_path: Path, *, follow_symlinks: bool = False, required_tfs: Optional[Sequence[str]] = None) -> None:
    cmd = [sys.executable, "-m", "octa.support.ops.universe_preflight", "--root", str(root), "--strict", "--out", str(preflight_out)]
    if required_tfs:
        cmd += ["--required-tfs", ",".join(str(t) for t in required_tfs)]
    if follow_symlinks:
        cmd.append("--follow-symlinks")
    _log(log_path, f"[cmd] {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    _log(log_path, res.stdout)
    if res.stderr:
        _log(log_path, res.stderr)
    if res.returncode != 0:
        raise RuntimeError(f"Preflight failed with code {res.returncode}")


def _check_altdata_cache(log_path: Path) -> Dict[str, Any]:
    cfg = load_altdat_config()
    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        return {"enabled": False, "degraded": False, "missing_sources": [], "total_sources": 0}
    cache_root = (cfg.get("storage") or {}).get("root")
    base = resolve_cache_root(cache_root)
    sources = cfg.get("sources") or {}
    missing: List[str] = []
    total = 0
    for name, scfg in sources.items():
        if not isinstance(scfg, dict) or not bool(scfg.get("enabled", False)):
            continue
        total += 1
        src_dir = base / name
        if not src_dir.exists() or not list(src_dir.rglob("*.json")):
            missing.append(name)
    degraded = bool(total > 0 and (len(missing) / float(total)) > 0.5)
    if missing:
        msg = f"altdata_cache_missing_sources:{','.join(sorted(missing))} total={total} degraded={degraded}"
        _log(log_path, msg)
    return {"enabled": True, "degraded": degraded, "missing_sources": sorted(missing), "total_sources": int(total)}


def _write_hashes(out_dir: Path, summary_path: Path, manifest_path: Path, results_dir: Path) -> Path:
    lines: List[str] = []
    extra = [
        out_dir / "run_manifest.json",
        out_dir / "input_symbols_report.json",
        out_dir / "logs" / "runner.log",
    ]
    for p in [summary_path, manifest_path, *extra]:
        if p.exists():
            lines.append(f"{_sha256_file(p)}  {p}")
    for p in sorted(results_dir.glob("*.json")):
        lines.append(f"{_sha256_file(p)}  {p}")
    preflight_dir = out_dir / "preflight"
    if preflight_dir.exists():
        for p in sorted(preflight_dir.rglob("*")):
            if p.is_file():
                lines.append(f"{_sha256_file(p)}  {p}")
    for subdir in ("exceptions", "gate_unexplained", "errors"):
        d = out_dir / subdir
        if d.exists():
            for p in sorted(d.rglob("*")):
                if p.is_file():
                    lines.append(f"{_sha256_file(p)}  {p}")
    hashes_path = out_dir / "hashes.sha256"
    hashes_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return hashes_path


def _promote_to_paper_ready(
    symbol: str,
    stages: List[Dict[str, Any]],
    paper_root: Path,
    run_id: str,
) -> List[str]:
    from datetime import datetime, timezone as _tz

    paper_root.mkdir(parents=True, exist_ok=True)
    out_paths: List[str] = []
    timeframes_promoted: List[str] = []
    per_tf_metrics: Dict[str, Any] = {}

    for stage in stages:
        tf = stage.get("timeframe")
        if not tf:
            continue
        stage_dir = paper_root / symbol / str(tf)
        stage_dir.mkdir(parents=True, exist_ok=True)
        artifacts = stage.get("model_artifacts") or []
        for p in artifacts:
            src = Path(p)
            if not src.exists():
                continue
            dst = stage_dir / src.name
            if dst.exists():
                continue
            try:
                os.link(src, dst)
            except Exception:
                shutil.copy2(src, dst)
            out_paths.append(str(dst))
        # copy feature schema if present
        schema = stage.get("features_used")
        if schema:
            schema_path = stage_dir / "feature_schema.json"
            schema_path.write_text(json.dumps({"features": list(schema), "run_id": run_id}, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
            out_paths.append(str(schema_path))
        # copy metrics summary
        metrics = stage.get("metrics_summary") or {}
        training_window = stage.get("training_window") or {}
        if isinstance(training_window, dict):
            metrics = {**metrics, "training_window": training_window}
        metrics_path = stage_dir / "metrics.json"
        metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        out_paths.append(str(metrics_path))
        timeframes_promoted.append(str(tf))
        per_tf_metrics[str(tf)] = metrics

    # Write ensemble_manifest.json at the symbol level
    sym_dir = paper_root / symbol
    sym_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": "v0.0.0",
        "symbol": symbol,
        "architecture": "regime_ensemble",
        "run_id": run_id,
        "created_at": datetime.now(_tz.utc).isoformat(),
        "timeframes": timeframes_promoted,
        "per_tf_metrics": per_tf_metrics,
        "submodels": {},  # populated by regime_ensemble training path; empty for cascade-only promotion
    }
    manifest_path = sym_dir / "ensemble_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    out_paths.append(str(manifest_path))

    return out_paths


@dataclass
class RunSettings:
    root: Path
    preflight_out: Path
    evidence_dir: Path
    batch_size: int
    max_symbols: int
    resume: bool
    start_at: Optional[str]
    dry_run: bool
    config_path: Optional[str]
    skip_preflight: bool = False
    follow_symlinks: bool = False
    asset_classes: Optional[Tuple[str, ...]] = None
    promote_required_tfs: Tuple[str, ...] = ("1D", "1H")
    paper_registry_dir: Optional[Path] = None  # None = skip paper_ready promotion (safe default for tests)
    symbols_override: Optional[List[str]] = None
    cascade_timeframes: Optional[Tuple[str, ...]] = None
    symbols_requested_explicitly: bool = False
    symbols_file_path: Optional[str] = None
    symbol_timeout_sec: Optional[int] = None
    symbol_timeout_grace_sec: int = 60


def _train_fn_supports_isolation(train_fn: Callable[..., Tuple[List[Any], Dict[str, Any]]]) -> bool:
    qualname = str(getattr(train_fn, "__qualname__", "") or "")
    if "<locals>" in qualname:
        return False
    return bool(getattr(train_fn, "__module__", None))


def _resolve_symbol_timeout_sec(settings: RunSettings) -> Optional[int]:
    if settings.symbol_timeout_sec is not None:
        try:
            if int(settings.symbol_timeout_sec) > 0:
                return int(settings.symbol_timeout_sec)
            return None
        except Exception:
            return None
    try:
        cfg = load_config(settings.config_path or "octa_training/config/training.yaml")
        raw = getattr(getattr(cfg, "tuning", None), "timeout_sec", None)
        if raw is None or str(raw).strip() == "":
            return None
        base = int(float(raw))
        if base <= 0:
            return None
        grace = max(0, int(settings.symbol_timeout_grace_sec))
        return base + grace
    except Exception:
        return None


def _symbol_train_child(
    result_queue: Any,
    train_fn: Callable[..., Tuple[List[Any], Dict[str, Any]]],
    train_kwargs: Dict[str, Any],
) -> None:
    try:
        decisions, metrics_by_tf = train_fn(**train_kwargs)
        result_queue.put(
            {
                "status": "ok",
                "decisions": decisions,
                "metrics_by_tf": metrics_by_tf,
            }
        )
    except BaseException as exc:
        result_queue.put(
            {
                "status": "exception",
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            }
        )


def _run_symbol_training_bounded(
    *,
    train_fn: Callable[..., Tuple[List[Any], Dict[str, Any]]],
    train_kwargs: Dict[str, Any],
    timeout_sec: Optional[int],
) -> Tuple[List[Any], Dict[str, Any]]:
    if timeout_sec is None or timeout_sec <= 0 or not _train_fn_supports_isolation(train_fn):
        return train_fn(**train_kwargs)

    method = "fork" if "fork" in multiprocessing.get_all_start_methods() else "spawn"
    ctx = multiprocessing.get_context(method)
    result_queue = ctx.Queue(maxsize=1)
    proc = ctx.Process(target=_symbol_train_child, args=(result_queue, train_fn, train_kwargs))
    proc.start()
    proc.join(float(timeout_sec))

    if proc.is_alive():
        proc.terminate()
        proc.join(10.0)
        if proc.is_alive():
            proc.kill()
            proc.join(5.0)
        raise SymbolTrainingTimeout(f"symbol_training_timeout:{int(timeout_sec)}s")

    try:
        payload = result_queue.get_nowait()
    except queue.Empty as exc:
        raise RuntimeError(f"symbol_training_child_exit_without_result:exitcode={proc.exitcode}") from exc

    status = str(payload.get("status") or "")
    if status == "ok":
        return payload.get("decisions") or [], payload.get("metrics_by_tf") or {}
    if status == "exception":
        raise RuntimeError(
            f"symbol_training_child_exception:{payload.get('exception_type')}:{payload.get('message')}"
        )
    raise RuntimeError(f"symbol_training_child_invalid_payload:{status or 'missing_status'}")


def _parse_symbols_arg(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    out: List[str] = []
    for part in str(raw).split(","):
        sym = part.strip().upper()
        if sym:
            out.append(sym)
    return out


def _load_symbols_file(path: Optional[str]) -> List[str]:
    if not path:
        return []
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    out: List[str] = []
    for line in lines:
        if not line.strip():
            continue
        for part in line.split(","):
            sym = part.strip().upper()
            if sym:
                out.append(sym)
    return out


def run_full_cascade(
    settings: RunSettings,
    train_fn: Callable[..., Tuple[List[Any], Dict[str, Any]]],
) -> Dict[str, Any]:
    log_path = settings.evidence_dir / "logs" / "runner.log"
    manifest_path = settings.evidence_dir / "manifest.jsonl"
    results_dir = settings.evidence_dir / "results"
    summary_path = settings.evidence_dir / "summary.json"

    settings.evidence_dir.mkdir(parents=True, exist_ok=True)

    # Governance hash-chain: emit TRAINING_RUN start event.
    # Fail-open: a broken GovernanceAudit must never abort training.
    _gov_run_id = str(settings.evidence_dir.name).strip() or _run_id()
    _gov_audit: Optional[GovernanceAudit] = None
    try:
        _gov_audit = GovernanceAudit(run_id=_gov_run_id)
        _gov_audit.emit(
            EVENT_TRAINING_RUN,
            {
                "phase": "start",
                "run_id": _gov_run_id,
                "config_path": settings.config_path,
                "dry_run": settings.dry_run,
                "max_symbols": settings.max_symbols,
                "promote_required_tfs": list(settings.promote_required_tfs),
            },
        )
    except Exception:
        _gov_audit = None

    os.environ["OKTA_ALTDATA_OFFLINE_ONLY"] = "1"
    os.environ["OKTA_ALTDATA_ENABLED"] = "1"

    error_reason: Optional[str] = None
    total = 0
    symbols: List[str] = []
    inventory: Dict[str, Dict[str, Any]] = {}
    passed = 0
    failed = 0
    timed_out = 0
    total_time = 0.0
    altdata_run_meta: Dict[str, Any] = {"enabled": False, "degraded": False, "missing_sources": [], "total_sources": 0}
    training_regime = "institutional_production"
    promotion_allowed = True
    input_symbols_report: Dict[str, Any] = {
        "symbols_requested_explicitly": bool(settings.symbols_requested_explicitly),
        "requested_count": 0,
        "accepted_count": 0,
        "rejected_count": 0,
        "duplicates_removed_count": 0,
        "invalid_format_count": 0,
        "missing_count": 0,
        "accepted_symbols": [],
        "duplicates_removed": [],
        "invalid_format_symbols": [],
        "rejected_symbols": [],
        "symbols_file_path": settings.symbols_file_path,
    }
    hard_blockers: List[str] = []
    warnings: List[str] = []
    current_symbol: Optional[str] = None
    current_symbol_asset_class: str = "unknown"
    current_symbol_started_at: Optional[str] = None
    finalized_symbols: set[str] = set()
    run_exception: Optional[BaseException] = None
    outcome_counts: Dict[str, int] = {
        "trained_successfully": 0,
        "trained_but_invalid": 0,
        "skipped": 0,
        "failed": 0,
        "dry_run": 0,
    }
    artifacts_valid = 0
    artifacts_invalid = 0
    symbol_timeout_sec = _resolve_symbol_timeout_sec(settings)

    previous_sigterm = signal.getsignal(signal.SIGTERM)
    previous_sigint = signal.getsignal(signal.SIGINT)

    def _abort_handler(signum, _frame):  # type: ignore[no-untyped-def]
        signame = signal.Signals(signum).name
        raise RunAbortRequested(f"signal_abort:{signame}")

    signal.signal(signal.SIGTERM, _abort_handler)
    signal.signal(signal.SIGINT, _abort_handler)

    try:
        cfg_for_policy = load_config(settings.config_path or "octa_training/config/training.yaml")
        training_regime = str(getattr(cfg_for_policy, "regime", "institutional_production") or "institutional_production").strip() or "institutional_production"
        promotion_allowed = training_regime != "foundation_validation"
        active_prototype_policy = resolve_active_prototype_policy(cfg_for_policy)
        prototype_asset_classes = prototype_allowed_asset_classes(cfg_for_policy)
    except Exception:
        cfg_for_policy = None
        active_prototype_policy = None
        prototype_asset_classes = None

    try:
        if not settings.skip_preflight:
            _run_preflight(settings.root, settings.preflight_out, log_path, follow_symlinks=bool(settings.follow_symlinks), required_tfs=settings.cascade_timeframes)

        files = _discover_preflight_files(settings.preflight_out)
        symbols = _load_trainable_symbols(files["trainable"])
        inventory = _load_inventory(files["inventory"])
        selected_asset_classes = _normalize_asset_class_filter(settings.asset_classes)
        if prototype_asset_classes:
            prototype_allowed_set = set(str(v).strip().lower() for v in prototype_asset_classes if str(v).strip())
            before_count = len(symbols)
            symbols = [s for s in symbols if str((inventory.get(s) or {}).get("asset_class", "unknown")).lower() in prototype_allowed_set]
            _log(
                log_path,
                f"[info] active_prototype_policy={active_prototype_policy.name if active_prototype_policy else 'unknown'} allowed_asset_classes={sorted(prototype_allowed_set)} before_count={before_count} after_count={len(symbols)}",
            )
        if selected_asset_classes:
            before_count = len(symbols)
            selected_set = set(selected_asset_classes)
            symbols = [s for s in symbols if str((inventory.get(s) or {}).get("asset_class", "unknown")).lower() in selected_set]
            _log(log_path, f"[info] selected_asset_classes={list(selected_asset_classes)} before_count={before_count} after_count={len(symbols)}")

        explicit_scope_requested = bool(settings.symbols_requested_explicitly or settings.symbols_override is not None)
        if explicit_scope_requested:
            requested = list(settings.symbols_override or [])
            accepted, request_report, rejected_rows = _build_symbol_request_report(
                requested,
                trainable_symbols=symbols,
                inventory=inventory,
                allowed_asset_classes=prototype_asset_classes,
            )
            input_symbols_report = {**input_symbols_report, **request_report}
            results_dir.mkdir(parents=True, exist_ok=True)
            for row in rejected_rows:
                sym = str(row.get("symbol", "")).upper() or "__INVALID__"
                reject_reason = str(row.get("reason", "symbol_request_rejected"))
                result = {
                    "symbol": sym,
                    "status": "FAIL",
                    "reason": reject_reason,
                    "detail": {"requested": True, **(row.get("detail") or {})},
                    "stages": [],
                    "training_outcome": "failed" if reject_reason == "invalid_requested_symbol_format" else "skipped",
                    "artifact_summary": {"valid_tradeable_artifacts": 0, "invalid_tradeable_artifacts": 0},
                    "started_at": _utc_now_iso(),
                    "ended_at": _utc_now_iso(),
                }
                _write_json(results_dir / f"{sym}.json", result)
                _append_jsonl(manifest_path, {"symbol": sym, "status": "FAIL", "reason": reject_reason})
                if result["training_outcome"] == "failed":
                    outcome_counts["failed"] += 1
                    failed += 1
                else:
                    outcome_counts["skipped"] += 1
            symbols = accepted
            _write_json(settings.evidence_dir / "input_symbols_report.json", input_symbols_report)
        elif settings.symbols_requested_explicitly:
            input_symbols_report["requested_count"] = 0
            _write_json(settings.evidence_dir / "input_symbols_report.json", input_symbols_report)

        if explicit_scope_requested and not symbols:
            error_reason = "no_valid_requested_symbols"
            hard_blockers.append("no_valid_requested_symbols")
            start_ts = _utc_now()
            _log(log_path, "[error] no_valid_requested_symbols")

        if settings.start_at:
            start = settings.start_at.strip().upper()
            if start in symbols:
                symbols = symbols[symbols.index(start) :]

        done: set[str] = set()
        if settings.resume and manifest_path.exists():
            for line in manifest_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                raw = json.loads(line)
                sym = str(raw.get("symbol", "")).upper()
                status = str(raw.get("status", "")).upper()
                if sym and status in {"PASS", "FAIL", "DRY_RUN"}:
                    done.add(sym)
            symbols = [s for s in symbols if s not in done]

        if settings.max_symbols and settings.max_symbols > 0:
            symbols = symbols[: settings.max_symbols]

        total = len(symbols)
        start_ts = _utc_now()
        _log(log_path, f"[info] trainable_symbols={total}")

        # v0.0.0 Pre-Screening: eliminate dead-end symbols before training loop.
        # Activated when cfg.prescreening.enabled == True (set in sweep YAML).
        # Symbols that fail are logged as PRESCREENED_OUT and excluded from training.
        try:
            # Load training config to access prescreening settings.
            # run_full_cascade() does not load cfg directly — it passes config_path
            # to train_fn per-symbol.  Load it here just for prescreening.
            try:
                from octa_training.core.config import load_config as _load_train_cfg
                cfg = _load_train_cfg(settings.config_path or "octa_training/config/training.yaml")
            except Exception:
                cfg = None
            _ps_cfg = getattr(cfg, "prescreening", None)
            if _ps_cfg is not None and bool(getattr(_ps_cfg, "enabled", False)):
                from octa_training.core.prescreening import (
                    prescreen_universe,
                    REASON_INSUFFICIENT_HISTORY,
                    REASON_PRICE_TOO_LOW,
                    REASON_VOLUME_TOO_LOW,
                    REASON_WARRANT_OR_RIGHTS,
                    REASON_INSUFFICIENT_REGIME_DIVERSITY,
                )
                _ps_results = prescreen_universe(
                    symbols=symbols,
                    inventory=inventory,
                    cfg=cfg,
                    log_fn=lambda msg: _log(log_path, f"[prescreening] {msg}"),
                )
                _prescreened_out = [s for s, r in _ps_results.items() if not r.passed]
                if _prescreened_out:
                    for _sym in _prescreened_out:
                        _sr = _ps_results[_sym]
                        _log(log_path, f"[stage] symbol={_sym} asset_class={str((inventory.get(_sym) or {}).get('asset_class', 'unknown'))} tf=1D status=PRESCREENED_OUT reason={_sr.reason} pf=None sharpe=None cagr=None max_dd=None n_trades=None artifacts_written=False")
                        _log(log_path, f"[stage] symbol={_sym} asset_class={str((inventory.get(_sym) or {}).get('asset_class', 'unknown'))} tf=1H status=SKIP reason=cascade_previous_not_pass pf=None sharpe=None cagr=None max_dd=None n_trades=None artifacts_written=False")
                        outcome_counts["skipped"] = outcome_counts.get("skipped", 0) + 1
                    symbols = [s for s in symbols if s not in set(_prescreened_out)]
                    total = len(symbols)
                    _log(log_path, f"[info] prescreening_complete prescreened_out={len(_prescreened_out)} remaining={total}")
        except Exception as _ps_exc:
            _log(log_path, f"[warn] prescreening_error={_ps_exc} — continuing without pre-screening")
    except Exception as exc:
        error_reason = f"preflight_exception:{exc}"
        start_ts = _utc_now()

    try:
        if error_reason is None:
            altdata_run_meta = _check_altdata_cache(log_path)
        else:
            final_exit_code = _compute_exit_code(error_reason=error_reason, outcome_counts=outcome_counts, hard_blockers=hard_blockers)
            summary = {
                "run_id": settings.evidence_dir.name,
                "started_at": start_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": _utc_now_iso(),
                "training_regime": training_regime,
                "total_trainable": total,
                "passed": passed,
                "failed": failed,
                "timed_out": timed_out,
                "avg_time_per_symbol": 0.0,
                "error": error_reason,
                "altdata_degraded": bool(altdata_run_meta.get("degraded", False)),
                "altdata_run_meta": _safe_json_obj(altdata_run_meta),
                "input_symbols_requested": int(input_symbols_report.get("requested_count", 0) or 0),
                "input_symbols_accepted": int(input_symbols_report.get("accepted_count", 0) or 0),
                "input_symbols_rejected": int(input_symbols_report.get("rejected_count", 0) or 0),
                "input_symbols_report": _safe_json_obj(input_symbols_report),
                "outcome_counts": dict(sorted(outcome_counts.items())),
                "artifacts_valid": int(artifacts_valid),
                "artifacts_invalid": int(artifacts_invalid),
                "hard_blockers": sorted(dict.fromkeys(hard_blockers)),
                "warnings": sorted(dict.fromkeys(warnings)),
                "final_verdict": "blocked_fail_closed",
                "exit_code": int(final_exit_code),
            }
            _write_json(summary_path, summary)
            _write_hashes(settings.evidence_dir, summary_path, manifest_path, results_dir)
            return {
                **summary,
            }
        for i in range(0, total, settings.batch_size):
            batch = symbols[i : i + settings.batch_size]
            for sym in batch:
                sym_start = time.time()
                if settings.dry_run:
                    result = {
                        "symbol": sym,
                        "status": "DRY_RUN",
                        "reason": "dry_run",
                        "stages": [],
                        "started_at": _utc_now_iso(),
                        "ended_at": _utc_now_iso(),
                    }
                    results_dir.mkdir(parents=True, exist_ok=True)
                    _write_json(results_dir / f"{sym}.json", result)
                    _append_jsonl(manifest_path, {"symbol": sym, "status": "DRY_RUN", "reason": "dry_run"})
                    continue

                cascade_tfs = settings.cascade_timeframes or DEFAULT_TIMEFRAMES
                parquet_paths = _build_parquet_paths(sym, inventory, tfs=cascade_tfs)
                if len(parquet_paths) != len(cascade_tfs):
                    result = {
                        "symbol": sym,
                        "status": "FAIL",
                        "reason": "missing_parquet_paths",
                        "stages": [],
                        "started_at": _utc_now_iso(),
                        "ended_at": _utc_now_iso(),
                    }
                    results_dir.mkdir(parents=True, exist_ok=True)
                    _write_json(results_dir / f"{sym}.json", result)
                    _append_jsonl(manifest_path, {"symbol": sym, "status": "FAIL", "reason": "missing_parquet_paths"})
                    failed += 1
                    continue

                run_id = settings.evidence_dir.name
                model_root = Path("octa") / "var" / "models" / "runs" / run_id / sym
                symbol_asset_class = str((inventory.get(sym) or {}).get("asset_class", "unknown")).strip().lower() or "unknown"
                current_symbol = sym
                current_symbol_asset_class = symbol_asset_class
                current_symbol_started_at = _utc_now_iso()
                _log(log_path, f"[train] symbol={sym} asset_class={symbol_asset_class} run_id={run_id}")
                try:
                    decisions, metrics_by_tf = _run_symbol_training_bounded(
                        train_fn=train_fn,
                        timeout_sec=symbol_timeout_sec,
                        train_kwargs={
                            "run_id": run_id,
                            "config_path": settings.config_path or "octa_training/config/training.yaml",
                            "symbol": sym,
                            "asset_class": symbol_asset_class,
                            "parquet_paths": parquet_paths,
                            "cascade": CascadePolicy(order=list(cascade_tfs)),
                            "safe_mode": False,
                            "reports_dir": str(settings.evidence_dir),
                            "model_root": str(model_root),
                        },
                    )
                    stages, ok, top_reason, top_detail = _normalize_decisions(
                        decisions,
                        metrics_by_tf,
                        expected_symbol=sym,
                        default_asset_class=symbol_asset_class,
                        cascade_tfs=cascade_tfs,
                    )
                    status = "PASS" if ok else "FAIL"
                    reason = None if ok else (top_reason or "stage_failed")
                except RunAbortRequested:
                    raise
                except SymbolTrainingTimeout as exc:
                    tf = DEFAULT_TIMEFRAMES[0]
                    _log(log_path, f"[error] symbol={sym} tf={tf} reason=symbol_training_timeout timeout_sec={symbol_timeout_sec}")
                    stages = [
                        {
                            "timeframe": tf,
                            "asset_class": symbol_asset_class,
                            "status": "TRAIN_ERROR",
                            "reason": "symbol_training_timeout",
                            "metrics_summary": {},
                            "model_artifacts": [],
                            "features_used": [],
                            "altdata_sources_used": [],
                            "altdata_enabled": False,
                            "training_window": None,
                            "gate_summary": {},
                            "gate_result": None,
                            "error_type": type(exc).__name__,
                            "error_message": _truncate_text(str(exc), MAX_EXCEPTION_MESSAGE_CHARS),
                            "exception_ref": None,
                        }
                    ]
                    status = "FAIL"
                    reason = "symbol_training_timeout"
                    top_detail = {
                        "timeframe": tf,
                        "timeout_sec": int(symbol_timeout_sec or 0),
                        "error_type": type(exc).__name__,
                    }
                    metrics_by_tf = {}
                    timed_out += 1
                except Exception as exc:
                    tb = traceback.format_exc()
                    tf = DEFAULT_TIMEFRAMES[0]
                    exception_ref = _write_train_exception_artifacts(
                        settings.evidence_dir,
                        symbol=sym,
                        timeframe=tf,
                        exception_type=type(exc).__name__,
                        message=str(exc),
                        traceback_full=tb,
                        where="octa.support.ops.run_full_cascade_training_from_parquets:run_full_cascade",
                    )
                    _log(log_path, f"[error] symbol={sym} tf={tf} reason=train_error exception_type={type(exc).__name__}")
                    stages = [
                        {
                            "timeframe": tf,
                            "asset_class": symbol_asset_class,
                            "status": "TRAIN_ERROR",
                            "reason": "train_error",
                            "metrics_summary": {},
                            "model_artifacts": [],
                            "features_used": [],
                            "altdata_sources_used": [],
                            "altdata_enabled": False,
                            "training_window": None,
                            "gate_summary": {},
                            "gate_result": None,
                            "error_type": type(exc).__name__,
                            "error_message": _truncate_text(str(exc), MAX_EXCEPTION_MESSAGE_CHARS),
                            "exception_ref": exception_ref,
                        }
                    ]
                    status = "FAIL"
                    reason = "train_error"
                    top_detail = {
                        "timeframe": tf,
                        "exception_ref": exception_ref,
                        "error_type": type(exc).__name__,
                    }
                    metrics_by_tf = {}

                cross_tf_meta: Dict[str, Any]
                try:
                    cross_tf_meta = evaluate_cross_timeframe_consistency(stages)
                except Exception as exc:
                    cross_tf_meta = {
                        "executed": True,
                        "passed": False,
                        "reason": "cross_tf_gate_exception",
                        "error": _truncate_text(str(exc), MAX_EXCEPTION_MESSAGE_CHARS),
                        "checks": [],
                    }
                if not bool(cross_tf_meta.get("executed", False)):
                    if status == "PASS":
                        status = "FAIL"
                        reason = "cross_tf_gate_not_executed"
                    if top_detail is None:
                        top_detail = {"cross_tf_meta": _safe_json_obj(cross_tf_meta)}
                elif status == "PASS" and not bool(cross_tf_meta.get("passed", False)):
                    status = "FAIL"
                    reason = "cross_tf_inconsistent"
                    if top_detail is None:
                        top_detail = {}
                    if isinstance(top_detail, dict):
                        top_detail["cross_tf_meta"] = _safe_json_obj(cross_tf_meta)

                for stage in stages:
                    stage_tf = str(stage.get("timeframe") or "")
                    metrics = stage.get("metrics_summary") or {}
                    artifacts = stage.get("model_artifacts") or []
                    ok_art, _ = _artifacts_valid(artifacts)
                    artifacts_written = bool(ok_art)
                    stage_msg = (
                        f"[stage] symbol={sym} asset_class={stage.get('asset_class', symbol_asset_class)} tf={stage.get('timeframe')} "
                        f"status={stage.get('status')} reason={stage.get('reason')} "
                        f"pf={metrics.get('profit_factor')} sharpe={metrics.get('sharpe')} "
                        f"cagr={metrics.get('cagr')} max_dd={metrics.get('max_drawdown')} "
                        f"n_trades={metrics.get('n_trades')} artifacts_written={artifacts_written}"
                    )
                    _log(log_path, stage_msg)
                    if stage.get("status") == "TRAIN_ERROR" and stage.get("reason") in {"train_error", "train_exception"} and not stage.get("exception_ref"):
                        detail = stage.get("decision_detail") or {}
                        err_text = str(detail.get("error") or stage.get("error_message") or "train_error")
                        tb_text = str(detail.get("traceback") or "")
                        if not tb_text and "Traceback" in err_text:
                            tb_text = err_text
                        exc_ref = _write_train_exception_artifacts(
                            settings.evidence_dir,
                            symbol=sym,
                            timeframe=stage_tf or "UNKNOWN",
                            exception_type=str(stage.get("error_type") or "RuntimeError"),
                            message=err_text,
                            traceback_full=tb_text,
                            where="octa.support.ops.run_full_cascade_training_from_parquets:train_fn_result",
                        )
                        stage["exception_ref"] = exc_ref
                        if isinstance(top_detail, dict) and top_detail.get("timeframe") == stage_tf:
                            top_detail["exception_ref"] = exc_ref
                    if stage.get("status") == "GATE_FAIL" and stage.get("reason") in {"gate_failed", "gate_failed_unexplained"}:
                        gate_summary = stage.get("gate_summary") or {}
                        failed_checks = gate_summary.get("failed_checks") or []
                        thresholds = gate_summary.get("thresholds")
                        if thresholds is None:
                            thresholds = gate_summary.get("effective_thresholds")
                        sample = failed_checks[:3]
                        _log(
                            log_path,
                            f"[gate] symbol={sym} tf={stage.get('timeframe')} reason={stage.get('reason')} failed_checks_count={len(failed_checks)} failed_checks_sample={sample} thresholds_summary={thresholds}",
                        )
                        if stage.get("reason") == "gate_failed_unexplained":
                            tf = stage_tf
                            gate_raw = (metrics_by_tf.get(tf) or {}).get("gate") if "metrics_by_tf" in locals() else None
                            gate_ref = _write_unexplained_gate_artifact(
                                settings.evidence_dir,
                                symbol=sym,
                                timeframe=tf or "UNKNOWN",
                                gate_summary=gate_summary,
                                gate_raw=gate_raw if isinstance(gate_raw, dict) else None,
                            )
                            stage["gate_unexplained_ref"] = gate_ref
                            if isinstance(top_detail, dict) and top_detail.get("timeframe") == tf:
                                top_detail["gate_unexplained_ref"] = gate_ref

                paper_ready = False
                paper_artifacts: List[str] = []
                paper_block_reason: Optional[str] = None
                required = set(settings.promote_required_tfs)
                stage_by_tf = {s.get("timeframe"): s for s in stages}
                if required and promotion_allowed and all(
                    stage_by_tf.get(tf, {}).get("status") == "PASS"
                    and bool((stage_by_tf.get(tf, {}).get("monte_carlo") or {}).get("passed", False))
                    for tf in required
                ):
                    paper_ready = True
                    if settings.paper_registry_dir is not None:
                        paper_artifacts = _promote_to_paper_ready(
                            sym,
                            stages,
                            settings.paper_registry_dir,
                            run_id,
                        )
                elif required and not promotion_allowed:
                    paper_block_reason = f"paper_promotion_blocked_for_regime:{training_regime}"

                result = {
                    "symbol": sym,
                    "asset_class": symbol_asset_class,
                    "asset_profiles": {
                        str(stage.get("timeframe")): {
                            "asset_profile": stage.get("asset_profile"),
                            "asset_profile_kind": stage.get("asset_profile_kind"),
                            "asset_profile_hash": stage.get("asset_profile_hash"),
                            "asset_profile_source": stage.get("asset_profile_source"),
                            "asset_profile_legacy_fallback": stage.get("asset_profile_legacy_fallback"),
                            "training_policy": stage.get("training_policy"),
                            "training_policy_source": stage.get("training_policy_source"),
                        }
                        for stage in stages
                    },
                    "training_policy": {
                        str(stage.get("timeframe")): {
                            "name": stage.get("training_policy"),
                            "source": stage.get("training_policy_source"),
                        }
                        for stage in stages
                    },
                    "training_regime": training_regime,
                    "status": status,
                    "reason": reason,
                    "detail": top_detail,
                    "cross_tf_meta": _safe_json_obj(cross_tf_meta),
                    "stages": stages,
                    "paper_ready": paper_ready,
                    "paper_artifacts": paper_artifacts,
                    "paper_block_reason": paper_block_reason,
                    "artifact_summary": {
                        "valid_tradeable_artifacts": sum(
                            1 for stage in stages if bool((stage.get("artifact_validation") or {}).get("valid", False))
                        ),
                        "invalid_tradeable_artifacts": sum(
                            1
                            for stage in stages
                            if stage.get("model_artifacts") and not bool((stage.get("artifact_validation") or {}).get("valid", False))
                        ),
                    },
                    "altdata_degraded": bool(altdata_run_meta.get("degraded", False)),
                    "altdata_run_meta": _safe_json_obj(altdata_run_meta),
                    "started_at": _utc_now_iso(),
                    "ended_at": _utc_now_iso(),
                }
                result["training_outcome"] = _classify_symbol_outcome(result)
                results_dir.mkdir(parents=True, exist_ok=True)
                _write_json(results_dir / f"{sym}.json", result)
                _append_jsonl(
                    manifest_path,
                    {
                        "symbol": sym,
                        "asset_class": symbol_asset_class,
                        "status": status,
                        "reason": reason,
                        "asset_profiles": result.get("asset_profiles"),
                        "training_policy": result.get("training_policy"),
                    },
                )
                finalized_symbols.add(sym)
                current_symbol = None
                current_symbol_started_at = None
                artifacts_valid += int((result.get("artifact_summary") or {}).get("valid_tradeable_artifacts", 0) or 0)
                artifacts_invalid += int((result.get("artifact_summary") or {}).get("invalid_tradeable_artifacts", 0) or 0)
                outcome_counts[result["training_outcome"]] = outcome_counts.get(result["training_outcome"], 0) + 1
                if status == "PASS":
                    passed += 1
                else:
                    failed += 1

                total_time += time.time() - sym_start
    except Exception as exc:
        run_exception = exc
        error_reason = f"run_exception:{exc}"
    finally:
        try:
            signal.signal(signal.SIGTERM, previous_sigterm)
            signal.signal(signal.SIGINT, previous_sigint)
        except Exception:
            pass

        if error_reason and current_symbol and current_symbol not in finalized_symbols:
            results_dir.mkdir(parents=True, exist_ok=True)
            fail_reason = "aborted_fail_closed" if isinstance(run_exception, RunAbortRequested) else "run_interrupted_before_symbol_finalization"
            result = {
                "symbol": current_symbol,
                "asset_class": current_symbol_asset_class,
                "training_regime": training_regime,
                "status": "FAIL",
                "reason": fail_reason,
                "detail": {
                    "error_reason": error_reason,
                    "error_type": type(run_exception).__name__ if run_exception is not None else None,
                },
                "cross_tf_meta": {},
                "stages": [],
                "paper_ready": False,
                "paper_artifacts": [],
                "paper_block_reason": None,
                "artifact_summary": {
                    "valid_tradeable_artifacts": 0,
                    "invalid_tradeable_artifacts": 0,
                },
                "altdata_degraded": bool(altdata_run_meta.get("degraded", False)),
                "altdata_run_meta": _safe_json_obj(altdata_run_meta),
                "started_at": current_symbol_started_at or _utc_now_iso(),
                "ended_at": _utc_now_iso(),
                "training_outcome": "failed",
            }
            _write_json(results_dir / f"{current_symbol}.json", result)
            _append_jsonl(manifest_path, {"symbol": current_symbol, "status": "FAIL", "reason": fail_reason})
            finalized_symbols.add(current_symbol)
            outcome_counts["failed"] = outcome_counts.get("failed", 0) + 1
            failed += 1

        if error_reason:
            _log(log_path, f"[error] {error_reason}")
        avg_time = total_time / (passed + failed) if (passed + failed) else 0.0
        final_exit_code = _compute_exit_code(error_reason=error_reason, outcome_counts=outcome_counts, hard_blockers=hard_blockers)
        if error_reason:
            final_verdict = "run_failed"
        elif final_exit_code == 0:
            final_verdict = "offline_training_ready"
        elif final_exit_code == 1:
            final_verdict = "completed_with_symbol_failures"
        else:
            final_verdict = "blocked_fail_closed"
        summary = {
            "run_id": settings.evidence_dir.name,
            "training_regime": training_regime,
            "started_at": start_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ended_at": _utc_now_iso(),
            "total_trainable": total,
            "passed": passed,
            "failed": failed,
            "timed_out": timed_out,
            "avg_time_per_symbol": avg_time,
            "error": error_reason,
            "altdata_degraded": bool(altdata_run_meta.get("degraded", False)),
            "altdata_run_meta": _safe_json_obj(altdata_run_meta),
            "input_symbols_requested": int(input_symbols_report.get("requested_count", 0) or 0),
            "input_symbols_accepted": int(input_symbols_report.get("accepted_count", 0) or 0),
            "input_symbols_rejected": int(input_symbols_report.get("rejected_count", 0) or 0),
            "input_symbols_report": _safe_json_obj(input_symbols_report),
            "outcome_counts": dict(sorted(outcome_counts.items())),
            "artifacts_valid": int(artifacts_valid),
            "artifacts_invalid": int(artifacts_invalid),
            "hard_blockers": sorted(dict.fromkeys(hard_blockers)),
            "warnings": sorted(dict.fromkeys(warnings)),
            "final_verdict": final_verdict,
            "exit_code": int(final_exit_code),
        }
        _write_json(summary_path, summary)
        _write_hashes(settings.evidence_dir, summary_path, manifest_path, results_dir)

        # Governance hash-chain: emit TRAINING_RUN end event.
        if _gov_audit is not None:
            try:
                _gov_audit.emit(
                    EVENT_TRAINING_RUN,
                    {
                        "phase": "end",
                        "run_id": _gov_run_id,
                        "final_verdict": final_verdict,
                        "exit_code": int(final_exit_code),
                        "passed": int(passed),
                        "failed": int(failed),
                        "timed_out": int(timed_out),
                        "paper_ready_count": int(outcome_counts.get("trained_successfully", 0)),
                        "error": error_reason,
                        "chain_path": str(_gov_audit.chain_path),
                    },
                )
            except Exception:
                pass

    return summary


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", default=None)
    p.add_argument("--evidence-dir", default=None)
    p.add_argument("--root", default="raw")
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--max-symbols", type=int, default=0)
    p.add_argument("--start-at", default=None)
    p.add_argument("--resume", action="store_true", default=False)
    p.add_argument("--dry-run", action="store_true", default=False)
    p.add_argument("--config", default=None)
    p.add_argument("--symbols", default=None)
    p.add_argument("--symbols-file", default=None)
    p.add_argument("--asset-class", dest="asset_classes", action="append", default=[])
    p.add_argument("--follow-symlinks", action="store_true", default=False)
    p.add_argument("--promote-required-tfs", default="1D,1H")
    p.add_argument("--paper-registry-dir", default=str(Path("octa") / "var" / "models" / "paper_ready"))
    p.add_argument("--cascade-timeframes", default=None, help="Comma-separated cascade TF order, e.g. '1D,4H,1H,30M,5M,1M'. Overrides config + DEFAULT_TIMEFRAMES.")
    args = p.parse_args()

    run_id = str(args.run_id).strip() if args.run_id else _run_id()
    evidence_dir = Path(args.evidence_dir) if args.evidence_dir else Path("octa") / "var" / "evidence" / run_id
    preflight_out = evidence_dir / "preflight"

    promote_tfs = tuple([t.strip().upper() for t in str(args.promote_required_tfs).split(",") if t.strip()])
    symbols_override = _parse_symbols_arg(args.symbols) + _load_symbols_file(args.symbols_file)
    cfg = load_config(args.config or "octa_training/config/training.yaml")

    # Read raw YAML once — used for scope-based auto-configuration below.
    _raw_yaml_scope: dict = {}
    try:
        import yaml as _yaml_scope_loader
        _raw_yaml_scope = _yaml_scope_loader.safe_load(
            Path(args.config or "octa_training/config/training.yaml").read_text(encoding="utf-8")
        ) or {}
    except Exception:
        pass

    cascade_tfs_raw = args.cascade_timeframes
    if cascade_tfs_raw:
        cascade_tfs: Optional[Tuple[str, ...]] = tuple([t.strip().upper() for t in cascade_tfs_raw.split(",") if t.strip()])
    elif getattr(cfg, "cascade_timeframes", None):
        cascade_tfs = tuple(cfg.cascade_timeframes)
    else:
        # Fall back to scope.timeframes from the config YAML so the cascade TFs
        # automatically match the allowed scope — prevents SCOPE_VIOLATION_TF.
        _scope_tfs = (_raw_yaml_scope.get("scope") or {}).get("timeframes")
        cascade_tfs = tuple(str(t).strip().upper() for t in _scope_tfs if str(t).strip()) if _scope_tfs else None

    # Auto-restrict preflight root to raw/Stock_parquet when scope is stock/equity-only.
    # Avoids scanning ~1026 non-equity parquets (ETF, FX, Futures, …) that will be
    # immediately SKIPped by the SCOPE_VIOLATION_ASSET_CLASS guard anyway.
    # Only fires when the user has not explicitly passed --root.
    if args.root == "raw":
        _scope_acs = {
            str(a).lower()
            for a in ((_raw_yaml_scope.get("scope") or {}).get("asset_classes") or [])
        }
        _EQUITY_TYPES = {"stock", "stocks", "equities", "equity"}
        if _scope_acs and _scope_acs.issubset(_EQUITY_TYPES):
            _stock_dir = Path("raw") / "Stock_parquet"
            if _stock_dir.exists():
                args.root = str(_stock_dir)

    settings = RunSettings(
        root=Path(args.root),
        preflight_out=preflight_out,
        evidence_dir=evidence_dir,
        batch_size=int(args.batch_size),
        max_symbols=int(args.max_symbols),
        resume=bool(args.resume),
        start_at=args.start_at,
        dry_run=bool(args.dry_run),
        config_path=args.config,
        follow_symlinks=bool(args.follow_symlinks),
        asset_classes=_normalize_asset_class_filter(args.asset_classes),
        promote_required_tfs=promote_tfs or ("1D", "1H"),
        paper_registry_dir=Path(args.paper_registry_dir),
        symbols_override=symbols_override or None,
        cascade_timeframes=cascade_tfs,
    )
    training_regime = str(getattr(cfg, "regime", "institutional_production") or "institutional_production").strip() or "institutional_production"

    manifest = {
        "run_id": run_id,
        "started_at": _utc_now_iso(),
        "python": _python_version(),
        "git_commit": _git_hash(),
        "argv": sys.argv,
        "training_regime": training_regime,
        "settings": {
            "root": str(settings.root),
            "preflight_out": str(settings.preflight_out),
            "batch_size": settings.batch_size,
            "max_symbols": settings.max_symbols,
            "resume": settings.resume,
            "start_at": settings.start_at,
            "dry_run": settings.dry_run,
            "config": settings.config_path,
            "symbols_override": settings.symbols_override,
            "follow_symlinks": bool(settings.follow_symlinks),
            "asset_classes": list(settings.asset_classes or ()),
            "cascade_order": list(settings.cascade_timeframes or DEFAULT_TIMEFRAMES),
            "pipeline_version": OCTA_VERSION,
            "promote_required_tfs": list(settings.promote_required_tfs),
            "paper_registry_dir": str(settings.paper_registry_dir),
            "asset_profile_routing": {
                "default_profile": str((getattr(cfg, "asset_defaults", {}) or {}).get("default_profile", "legacy")),
                "by_asset_class": dict((getattr(cfg, "asset_defaults", {}) or {}).get("by_asset_class", {}) or {}),
                "configured_profiles": sorted(list((getattr(cfg, "asset_profiles", {}) or {}).keys())),
            },
            "training_policy": {
                "prototype_enabled": bool(resolve_active_prototype_policy(cfg) is not None),
                "active_prototype_policy": getattr(resolve_active_prototype_policy(cfg), "name", None),
                "prototype_allowed_asset_classes": list(prototype_allowed_asset_classes(cfg) or ()),
            },
        },
    }
    _write_json(settings.evidence_dir / "run_manifest.json", manifest)

    run_full_cascade(settings, train_fn=run_cascade_training)


if __name__ == "__main__":
    main()
