from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import traceback
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from octa import __version__ as OCTA_VERSION
from octa.core.cascade.policies import DEFAULT_TIMEFRAMES
from octa_training.core.institutional_gates import evaluate_cross_timeframe_consistency
from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training
from octa.core.data.sources.altdata.orchestrator import load_altdat_config
from octa.core.data.sources.altdata.cache import resolve_cache_root


MAX_EXCEPTION_MESSAGE_CHARS = 2000
MAX_EXCEPTION_TRACEBACK_CHARS = 20000


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


def _load_inventory(path: Path) -> Dict[str, Dict[str, List[str]]]:
    inventory: Dict[str, Dict[str, List[str]]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        sym = str(raw.get("symbol", "")).upper()
        tfs = raw.get("tfs") or {}
        if sym:
            inventory[sym] = {str(tf).upper(): list(paths) for tf, paths in tfs.items()}
    return inventory


def _pick_rep(paths: Sequence[str]) -> Optional[str]:
    if not paths:
        return None
    return sorted(paths, key=lambda p: (len(p), p))[0]


def _build_parquet_paths(symbol: str, inventory: Dict[str, Dict[str, List[str]]]) -> Dict[str, str]:
    by_tf = inventory.get(symbol, {})
    out: Dict[str, str] = {}
    for tf in DEFAULT_TIMEFRAMES:
        rep = _pick_rep(by_tf.get(tf, []))
        if rep:
            out[tf] = rep
    return out


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
) -> Tuple[List[Dict[str, Any]], bool, Optional[str], Optional[Dict[str, Any]]]:
    out: List[Dict[str, Any]] = []
    prev_pass = True
    overall_pass = True
    top_fail_reason: Optional[str] = None
    top_detail: Optional[Dict[str, Any]] = None
    for tf in DEFAULT_TIMEFRAMES:
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
        if status == "PASS":
            ok_metrics, why_metrics = _metrics_valid(metrics)
            ok_artifacts, why_artifacts = _artifacts_valid(model_artifacts)
            if not ok_metrics:
                status = "GATE_FAIL"
                reason = why_metrics
            elif not ok_artifacts:
                status = "GATE_FAIL"
                reason = why_artifacts
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
                "decision_detail": decision_detail,
            }
        )
    return out, overall_pass, top_fail_reason, top_detail


def _run_preflight(root: Path, preflight_out: Path, log_path: Path) -> None:
    cmd = [sys.executable, "-m", "octa.support.ops.universe_preflight", "--root", str(root), "--strict", "--out", str(preflight_out)]
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
    paper_root.mkdir(parents=True, exist_ok=True)
    out_paths: List[str] = []
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
    promote_required_tfs: Tuple[str, ...] = ("1D", "1H")
    paper_registry_dir: Path = Path("octa") / "var" / "models" / "paper_ready"
    symbols_override: Optional[List[str]] = None


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

    os.environ["OKTA_ALTDATA_OFFLINE_ONLY"] = "1"
    os.environ["OKTA_ALTDATA_ENABLED"] = "1"

    error_reason: Optional[str] = None
    total = 0
    symbols: List[str] = []
    inventory: Dict[str, Dict[str, List[str]]] = {}
    passed = 0
    failed = 0
    timed_out = 0
    total_time = 0.0
    altdata_run_meta: Dict[str, Any] = {"enabled": False, "degraded": False, "missing_sources": [], "total_sources": 0}

    try:
        if not settings.skip_preflight:
            _run_preflight(settings.root, settings.preflight_out, log_path)

        files = _discover_preflight_files(settings.preflight_out)
        symbols = _load_trainable_symbols(files["trainable"])
        inventory = _load_inventory(files["inventory"])

        if settings.symbols_override:
            requested: List[str] = []
            seen: set[str] = set()
            for sym in settings.symbols_override:
                s = str(sym).strip().upper()
                if not s or s in seen:
                    continue
                seen.add(s)
                requested.append(s)

            trainable_set = set(symbols)
            inventory_set = set(inventory.keys())
            results_dir.mkdir(parents=True, exist_ok=True)
            for sym in requested:
                if sym not in trainable_set or sym not in inventory_set:
                    result = {
                        "symbol": sym,
                        "status": "FAIL",
                        "reason": "symbol_not_trainable_or_missing",
                        "detail": {"requested": True, "trainable": sym in trainable_set, "in_inventory": sym in inventory_set},
                        "stages": [],
                        "started_at": _utc_now_iso(),
                        "ended_at": _utc_now_iso(),
                    }
                    _write_json(results_dir / f"{sym}.json", result)
                    _append_jsonl(manifest_path, {"symbol": sym, "status": "FAIL", "reason": "symbol_not_trainable_or_missing"})
                    failed += 1
            symbols = [s for s in requested if s in trainable_set and s in inventory_set]

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
    except Exception as exc:
        error_reason = f"preflight_exception:{exc}"
        start_ts = _utc_now()

    try:
        if error_reason is None:
            altdata_run_meta = _check_altdata_cache(log_path)
        else:
            return {
                "run_id": settings.evidence_dir.name,
                "started_at": start_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": _utc_now_iso(),
                "total_trainable": total,
                "passed": passed,
                "failed": failed,
                "timed_out": timed_out,
                "avg_time_per_symbol": 0.0,
                "error": error_reason,
                "altdata_degraded": bool(altdata_run_meta.get("degraded", False)),
                "altdata_run_meta": _safe_json_obj(altdata_run_meta),
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

                parquet_paths = _build_parquet_paths(sym, inventory)
                if len(parquet_paths) != len(DEFAULT_TIMEFRAMES):
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
                _log(log_path, f"[train] symbol={sym} run_id={run_id}")
                try:
                    decisions, metrics_by_tf = train_fn(
                        run_id=run_id,
                        config_path=settings.config_path or "octa_training/config/training.yaml",
                        symbol=sym,
                        asset_class="unknown",
                        parquet_paths=parquet_paths,
                        cascade=CascadePolicy(order=list(DEFAULT_TIMEFRAMES)),
                        safe_mode=False,
                        reports_dir=str(settings.evidence_dir),
                        model_root=str(model_root),
                    )
                    stages, ok, top_reason, top_detail = _normalize_decisions(decisions, metrics_by_tf)
                    status = "PASS" if ok else "FAIL"
                    reason = None if ok else (top_reason or "stage_failed")
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
                        f"[stage] symbol={sym} tf={stage.get('timeframe')} "
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
                required = set(settings.promote_required_tfs)
                stage_by_tf = {s.get("timeframe"): s for s in stages}
                if required and all(
                    stage_by_tf.get(tf, {}).get("status") == "PASS"
                    and bool((stage_by_tf.get(tf, {}).get("monte_carlo") or {}).get("passed", False))
                    for tf in required
                ):
                    paper_ready = True
                    paper_artifacts = _promote_to_paper_ready(
                        sym,
                        stages,
                        settings.paper_registry_dir,
                        run_id,
                    )

                result = {
                    "symbol": sym,
                    "status": status,
                    "reason": reason,
                    "detail": top_detail,
                    "cross_tf_meta": _safe_json_obj(cross_tf_meta),
                    "stages": stages,
                    "paper_ready": paper_ready,
                    "paper_artifacts": paper_artifacts,
                    "altdata_degraded": bool(altdata_run_meta.get("degraded", False)),
                    "altdata_run_meta": _safe_json_obj(altdata_run_meta),
                    "started_at": _utc_now_iso(),
                    "ended_at": _utc_now_iso(),
                }
                results_dir.mkdir(parents=True, exist_ok=True)
                _write_json(results_dir / f"{sym}.json", result)
                _append_jsonl(manifest_path, {"symbol": sym, "status": status, "reason": reason})
                if status == "PASS":
                    passed += 1
                else:
                    failed += 1

                total_time += time.time() - sym_start
    except Exception as exc:
        error_reason = f"run_exception:{exc}"
    finally:
        if error_reason:
            _log(log_path, f"[error] {error_reason}")
        avg_time = total_time / (passed + failed) if (passed + failed) else 0.0
        summary = {
            "run_id": settings.evidence_dir.name,
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
        }
        _write_json(summary_path, summary)
        _write_hashes(settings.evidence_dir, summary_path, manifest_path, results_dir)

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
    p.add_argument("--promote-required-tfs", default="1D,1H")
    p.add_argument("--paper-registry-dir", default=str(Path("octa") / "var" / "models" / "paper_ready"))
    args = p.parse_args()

    run_id = str(args.run_id).strip() if args.run_id else _run_id()
    evidence_dir = Path(args.evidence_dir) if args.evidence_dir else Path("octa") / "var" / "evidence" / run_id
    preflight_out = evidence_dir / "preflight"

    promote_tfs = tuple([t.strip().upper() for t in str(args.promote_required_tfs).split(",") if t.strip()])
    symbols_override = _parse_symbols_arg(args.symbols) + _load_symbols_file(args.symbols_file)
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
        promote_required_tfs=promote_tfs or ("1D", "1H"),
        paper_registry_dir=Path(args.paper_registry_dir),
        symbols_override=symbols_override or None,
    )

    manifest = {
        "run_id": run_id,
        "started_at": _utc_now_iso(),
        "python": _python_version(),
        "git_commit": _git_hash(),
        "argv": sys.argv,
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
            "cascade_order": list(DEFAULT_TIMEFRAMES),
            "pipeline_version": OCTA_VERSION,
            "promote_required_tfs": list(settings.promote_required_tfs),
            "paper_registry_dir": str(settings.paper_registry_dir),
        },
    }
    _write_json(settings.evidence_dir / "run_manifest.json", manifest)

    run_full_cascade(settings, train_fn=run_cascade_training)


if __name__ == "__main__":
    main()
