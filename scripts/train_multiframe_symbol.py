#!/usr/bin/env python3
"""scripts/train_multiframe_symbol.py

Multi-timeframe training per symbol.

Strict cascade principle (risk-first, deterministic):
- Train 1D -> evaluate. Only if PASS, proceed to 1H.
- Train 1H -> evaluate. Only if PASS, proceed to 30m.
- Lower layers are never trained if the parent layer did not PASS.

Live trading is enabled ONLY via a deterministic gate-based matrix.
Higher timeframes dominate lower timeframes; lower TFs cannot override higher TFs.

Audit artifacts:
- Per (symbol, timeframe): decision.json
- Per timeframe: pass_symbols_<TF>.txt / fail_symbols_<TF>.txt / err_symbols_<TF>.txt
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    import fcntl  # type: ignore
except Exception:  # pragma: no cover
    fcntl = None

# Allow running as a script from the repo root without installing the package
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    import yaml
except Exception:
    yaml = None

from core.training_safety_lock import (
    TrainingSafetyLockError,
    assert_training_armed,
    emit_audit_log,
)
from octa_training.core.asset_class import infer_asset_class
from octa_training.core.config import TrainingConfig, load_config
from octa_training.core.device import detect_device
from octa_training.core.features import build_features
from octa_training.core.fs_utils import ensure_disk_space
from octa_training.core.io_parquet import (
    discover_parquets,
    inspect_parquet,
    load_parquet,
)
from octa_training.core.live_release_matrix import (
    determine_live_release,
    outcome_from_pipeline_dict,
)
from octa_training.core.models import train_models
from octa_training.core.notify import send_telegram
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.splits import SplitFold, walk_forward_splits
from octa_training.core.state import StateRegistry

TF_ORDER = ["1D", "1H", "30m", "5m", "1m"]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _decision_status(*, passed: bool, error: object | None) -> str:
    if error:
        return "ERR"
    return "PASS" if passed else "FAIL"


def _extract_gate_details(res) -> tuple[str | None, list[str]]:
    """Return (gate_version, reasons)."""
    gate_obj = getattr(res, "gate_result", None)
    gate_version = None
    reasons: list[str] = []

    try:
        gate_version = getattr(gate_obj, "gate_version", None)
    except Exception:
        gate_version = None

    try:
        raw = getattr(gate_obj, "reasons", None)
        if isinstance(raw, list):
            reasons = [str(r) for r in raw if r]
    except Exception:
        reasons = []

    return gate_version, reasons


def _read_symbol_set(path: Path) -> set[str]:
    if not path.exists():
        return set()
    out: set[str] = set()
    for line in path.read_text().splitlines():
        s = _norm_symbol(line)
        if s:
            out.add(s)
    return out


def _write_symbol_set(path: Path, symbols: set[str]) -> None:
    lines = sorted(symbols)
    path.write_text("\n".join(lines) + ("\n" if lines else ""))


def _update_tf_lists(run_dir: Path, tf: str, symbol_base: str, status: str) -> None:
    tf_norm = str(tf)
    sym = _norm_symbol(symbol_base)
    pass_p = run_dir / f"pass_symbols_{tf_norm}.txt"
    fail_p = run_dir / f"fail_symbols_{tf_norm}.txt"
    err_p = run_dir / f"err_symbols_{tf_norm}.txt"
    skip_p = run_dir / f"skip_symbols_{tf_norm}.txt"

    passed = _read_symbol_set(pass_p)
    failed = _read_symbol_set(fail_p)
    errored = _read_symbol_set(err_p)
    skipped = _read_symbol_set(skip_p)

    # Ensure uniqueness across lists.
    passed.discard(sym)
    failed.discard(sym)
    errored.discard(sym)
    skipped.discard(sym)

    if status == "PASS":
        passed.add(sym)
    elif status == "FAIL":
        failed.add(sym)
    elif str(status).upper().startswith("SKIP"):
        skipped.add(sym)
    else:
        errored.add(sym)

    _write_symbol_set(pass_p, passed)
    _write_symbol_set(fail_p, failed)
    _write_symbol_set(err_p, errored)
    _write_symbol_set(skip_p, skipped)


def _repair_pass_list_from_decisions(run_dir: Path, tf: str) -> None:
    tf_norm = str(tf)
    pass_p = run_dir / f"pass_symbols_{tf_norm}.txt"
    current = _read_symbol_set(pass_p)
    repaired: set[str] = set()
    for dec_path in run_dir.glob(f"*/{tf_norm}/decision.json"):
        try:
            payload = json.loads(dec_path.read_text())
        except Exception:
            continue
        status = str(payload.get("status", "")).upper()
        if status == "PASS":
            sym = _norm_symbol(payload.get("symbol") or dec_path.parent.parent.name)
            if sym:
                repaired.add(sym)
    if repaired and repaired != current:
        _write_symbol_set(pass_p, repaired)


def _decision_allows_missing_skip(run_dir: Path, tf: str, symbol_base: str) -> bool:
    tf_norm = str(tf)
    dec_path = run_dir / _norm_symbol(symbol_base) / tf_norm / "decision.json"
    if not dec_path.exists():
        return False
    try:
        payload = json.loads(dec_path.read_text())
    except Exception:
        return False
    status = str(payload.get("status", "")).upper()
    if not status.startswith("SKIP"):
        return False
    reasons = payload.get("fail_reasons") or payload.get("reason") or []
    if isinstance(reasons, list):
        reasons_txt = " ".join([str(r) for r in reasons])
    else:
        reasons_txt = str(reasons)
    return "missing_parquet" in reasons_txt


def _atomic_append_ndjson(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, default=str)
    # Use an explicit fd so O_APPEND is atomic at the syscall level.
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        if fcntl is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
            except Exception:
                pass
        os.write(fd, (line + "\n").encode("utf-8"))
    finally:
        try:
            if fcntl is not None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except Exception:
                    pass
        finally:
            os.close(fd)


def _parquet_row_count(path: Path) -> int | None:
    try:
        import pyarrow.parquet as pq  # type: ignore

        return int(pq.read_metadata(str(path)).num_rows)
    except Exception:
        return None


def _parquet_columns(path: Path) -> list[str] | None:
    try:
        import pyarrow.parquet as pq  # type: ignore

        schema = pq.read_schema(str(path))
        return [str(n) for n in schema.names]
    except Exception:
        return None


def _parquet_delisted_fast(path: Path) -> tuple[bool, str | None]:
    """Best-effort delisted detection from Parquet metadata only."""
    try:
        import pyarrow.parquet as pq  # type: ignore

        md = pq.read_metadata(str(path)).metadata
        if not md:
            return False, None
        if b"delisting_date" in md:
            try:
                return True, md[b"delisting_date"].decode("utf-8", errors="ignore")
            except Exception:
                return True, None
        if b"delisted" in md:
            try:
                v = md[b"delisted"].decode("utf-8", errors="ignore").strip().lower()
                if v.isdigit():
                    return bool(int(v)), None
                return v in {"1", "true", "yes"}, None
            except Exception:
                return True, None
        return False, None
    except Exception:
        return False, None


def _hf30m_status_from_decision(status: str) -> str:
    s = str(status or "").upper()
    if s == "PASS":
        return "PASS"
    if s == "FAIL":
        return "FAIL_HF_METRICS"
    if s.startswith("SKIP"):
        return "SKIP_30M_NOT_ELIGIBLE"
    return "ERROR"


def _write_decision(run_dir: Path, symbol_base: str, tf: str, payload: dict) -> Path:
    out_dir = run_dir / _norm_symbol(symbol_base) / str(tf)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "decision.json"
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=False, default=str) + "\n")
    return out_path


def _norm_symbol(s: str) -> str:
    return str(s or "").strip().upper().replace("-", "_")


def _timeframe_tokens_from_suffix(suffix: str) -> list[str]:
    suf = _norm_symbol(suffix)
    if suf in {"", "D", "1D", "1DAY", "DAILY"}:
        return ["_1D", "_1DAY", "_DAILY", "_FULL_1DAY", "_FULL_DAILY"]
    if suf in {"1H", "1HR", "1HOUR"}:
        return ["_1H", "_1HR", "_1HOUR", "_FULL_1HOUR", "_60MIN", "_60M"]
    if suf in {"30M", "30MIN"}:
        return ["_30M", "_30MIN", "_FULL_30MIN"]
    if suf in {"5M", "5MIN"}:
        return ["_5M", "_5MIN", "_FULL_5MIN"]
    if suf in {"1M", "1MIN"}:
        return ["_1M", "_1MIN", "_FULL_1MIN"]
    return ["_" + suf]


def _pipeline_result_to_dict(res):
    if res is None:
        return None

    def _get_metric(metrics_obj, key: str):
        if metrics_obj is None:
            return None
        try:
            if isinstance(metrics_obj, dict):
                return metrics_obj.get(key)
        except Exception:
            pass
        try:
            return getattr(metrics_obj, key)
        except Exception:
            return None

    metrics_obj = getattr(res, "metrics", None)
    gate_obj = getattr(res, "gate_result", None)
    out = {
        "symbol": getattr(res, "symbol", None),
        "run_id": getattr(res, "run_id", None),
        "passed": bool(getattr(res, "passed", False)),
        "error": getattr(res, "error", None),
        "pack_result": getattr(res, "pack_result", None),
        "gate_result": None,
        "metrics": {
            "n_trades": _get_metric(metrics_obj, "n_trades"),
            "sharpe": _get_metric(metrics_obj, "sharpe"),
            "sortino": _get_metric(metrics_obj, "sortino"),
            "sharpe_wf_std": _get_metric(metrics_obj, "sharpe_wf_std"),
            "max_drawdown": _get_metric(metrics_obj, "max_drawdown"),
            "cagr": _get_metric(metrics_obj, "cagr"),
            "avg_net_trade_return": _get_metric(metrics_obj, "avg_net_trade_return"),
            "metadata": None,
        },
    }

    try:
        meta_obj = _get_metric(metrics_obj, "metadata")
        if meta_obj is not None:
            if isinstance(meta_obj, dict):
                out["metrics"]["metadata"] = meta_obj
            elif hasattr(meta_obj, "model_dump"):
                out["metrics"]["metadata"] = meta_obj.model_dump()
            elif hasattr(meta_obj, "dict"):
                out["metrics"]["metadata"] = meta_obj.dict()
            else:
                out["metrics"]["metadata"] = {
                    "horizon": getattr(meta_obj, "horizon", None),
                    "bar_size": getattr(meta_obj, "bar_size", None),
                    "cost_bps": getattr(meta_obj, "cost_bps", None),
                    "spread_bps": getattr(meta_obj, "spread_bps", None),
                    "sample_start": getattr(meta_obj, "sample_start", None),
                    "sample_end": getattr(meta_obj, "sample_end", None),
                }
    except Exception:
        pass

    # Include gate details for debugging and HF iteration.
    try:
        if gate_obj is not None:
            if hasattr(gate_obj, 'model_dump'):
                out['gate_result'] = gate_obj.model_dump()
            elif hasattr(gate_obj, 'dict'):
                out['gate_result'] = gate_obj.dict()
            else:
                out['gate_result'] = {
                    'passed': getattr(gate_obj, 'passed', None),
                    'reasons': getattr(gate_obj, 'reasons', None),
                    'passed_checks': getattr(gate_obj, 'passed_checks', None),
                    'robustness': getattr(gate_obj, 'robustness', None),
                }
    except Exception:
        out['gate_result'] = None
    return out


def find_symbol_variant(discovered, base_symbol, suffix):
    base_in = _norm_symbol(base_symbol)
    suf = _norm_symbol(suffix)

    # Some datasets (e.g., Indices bundles) do not have a plain daily symbol like "DVS".
    # Instead they use variants like "DVS_FULL_1DAY". In that case, treat the first
    # underscore-separated token as the base root for matching.
    base_root = base_in.split("_", 1)[0] if "_" in base_in else base_in

    base_candidates = [base_in]
    if base_root and base_root != base_in:
        base_candidates.append(base_root)

    # 1) Exact match preference
    if not suf:
        for base in base_candidates:
            for d in discovered:
                if _norm_symbol(d.symbol) == base:
                    return d.symbol
    else:
        for base in base_candidates:
            exact = f"{base}_{suf}"
            for d in discovered:
                if _norm_symbol(d.symbol) == exact:
                    return d.symbol

    # 2) Heuristic match for common naming schemes (e.g. SYMBOL_full_1hour)
    tokens = _timeframe_tokens_from_suffix(suf)
    # For daily, also allow token-based matching (e.g. *_FULL_1DAY).
    if not suf:
        tokens = _timeframe_tokens_from_suffix("1D")
    best_sym = None
    best_score = -1
    for d in discovered:
        name = _norm_symbol(d.symbol)
        # Try against any base candidate.
        base = None
        for b in base_candidates:
            if name.startswith(b):
                base = b
                break
        if base is None:
            continue

        score = 50
        if name == base:
            score += 25
        if "FULL" in name:
            score += 5
        for t in tokens:
            if t in name:
                score += 30
        # small preference for shorter (less noisy) names once token matches
        score -= max(0, len(name) - len(base)) * 0.01
        if score > best_score:
            best_score = score
            best_sym = d.symbol

    return best_sym


def _deep_merge(dst: dict, src: dict):
    """Recursively merge src into dst (modifies dst)."""
    if not isinstance(dst, dict) or not isinstance(src, dict):
        return
    for k, v in src.items():
        if k in dst and isinstance(dst[k], dict) and isinstance(v, dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = v


def _build_layer_cfg(cfg: TrainingConfig, layer_overrides: dict | None) -> TrainingConfig:
    base_cfg = cfg.model_dump() if hasattr(cfg, 'model_dump') else (cfg.dict() if hasattr(cfg, 'dict') else {})
    if layer_overrides and isinstance(layer_overrides, dict):
        _deep_merge(base_cfg, layer_overrides)
    return TrainingConfig(**base_cfg)


def run_sequence(
    symbol_base,
    cfg,
    state,
    run_id="multirun",
    force: bool = False,
    include_5m: bool = False,
    include_1m: bool = False,
    continue_on_fail: bool = False,
    train_daily_regime: bool = True,
    gate_only: bool = False,
    mode: str = "train",
    config_raw: dict | None = None,
    layers: list[str] | None = None,
):
    # Enforce training safety lock before proceeding: global gate must exist and be ARMed.
    try:
        try:
            assert_training_armed(cfg, symbol_base, "1D")
        except TrainingSafetyLockError as e:
            # emit audit event and abort early (fail-closed)
            try:
                audit_p = Path(cfg.paths.reports_dir) / "gates" / "training_safety_audit.jsonl"
                emit_audit_log({"event": "training_blocked", "symbol": symbol_base, "timeframe": "1D", "reason": str(e), "run_id": str(run_id)}, audit_p)
            except Exception:
                pass
            return {"error": f"TRAINING BLOCKED BY SAFETY LOCK: {e}"}
    except Exception:
        # conservative: block training
        return {"error": "TRAINING BLOCKED BY SAFETY LOCK: unknown"}

    # load symbol->asset_class map if provided
    def load_asset_map():
        p_yaml = Path('assets/asset_map.yaml')
        p_json = Path('assets/asset_map.json')
        if p_yaml.exists():
            try:
                if yaml:
                    return yaml.safe_load(p_yaml.read_text()) or {}
                else:
                    return json.loads(p_yaml.read_text())
            except Exception:
                return {}
        if p_json.exists():
            try:
                return json.loads(p_json.read_text())
            except Exception:
                return {}
        return {}

    asset_map = load_asset_map()

    # discover available parquets
    discovered = discover_parquets(Path(cfg.paths.raw_dir), state=state)

    def _normalize_asset_class_local(label):
        if not label:
            return "unknown"
        v = str(label).strip().lower()
        if v in {"equity", "stock", "shares", "stocks"}:
            return "stock"
        if v in {"options", "option"}:
            return "option"
        if v in {"future", "futures"}:
            return "future"
        return v

    # Determine asset_class once per base symbol (prefer inference from parquet).
    asset_class_state = None
    try:
        asset_class_state = (state.get_symbol_state(symbol_base) or {}).get('asset_class')
    except Exception:
        asset_class_state = None

    inferred = None
    # Prefer daily parquet for inference if present; otherwise fall back to any variant.
    pinfo_for_infer = None
    try:
        sym_daily = find_symbol_variant(discovered, symbol_base, "")
        if sym_daily:
            pinfo_for_infer = [d for d in discovered if d.symbol == sym_daily][0]
    except Exception:
        pinfo_for_infer = None
    if pinfo_for_infer is None:
        try:
            pinfo_for_infer = [d for d in discovered if d.symbol.startswith(symbol_base)][0]
        except Exception:
            pinfo_for_infer = None

    if pinfo_for_infer is not None:
        cols = []
        try:
            meta = inspect_parquet(pinfo_for_infer.path)
            cols = meta.get('columns') or []
        except Exception:
            cols = []
        if not cols:
            try:
                df_tmp = load_parquet(pinfo_for_infer.path)
                cols = list(df_tmp.columns)
            except Exception:
                cols = []
        try:
            inferred = infer_asset_class(symbol_base, str(pinfo_for_infer.path), cols, cfg)
        except Exception:
            inferred = None

    asset_class_raw = inferred or "unknown"
    # Only use state/asset_map if inference couldn't decide.
    if _normalize_asset_class_local(asset_class_raw) == 'unknown':
        asset_class_raw = asset_class_state or asset_map.get(symbol_base) or asset_class_raw

    asset_class = _normalize_asset_class_local(asset_class_raw)

    # Persist asset_class for base symbol up front (used by pipeline + monitoring).
    try:
        state.update_symbol_state(symbol_base, asset_class=asset_class)
    except Exception:
        pass

    # Apply per-asset-class overlay once (pipeline also applies overlays, but daily regime path lives here).
    try:
        asset_cfg_file = Path('configs') / 'asset' / f"{asset_class}.yaml"
        if asset_cfg_file.exists() and yaml:
            raw_asset = yaml.safe_load(asset_cfg_file.read_text()) or {}
            base_cfg = cfg.model_dump() if hasattr(cfg, 'model_dump') else (cfg.dict() if hasattr(cfg, 'dict') else {})
            _deep_merge(base_cfg, raw_asset)
            cfg = TrainingConfig(**base_cfg)
            print(f"Applied asset config overlay '{asset_class}' from {asset_cfg_file}")
            try:
                state.update_symbol_state(symbol_base, asset_config_overlay_path=str(asset_cfg_file))
            except Exception:
                pass
    except Exception as e:
        print(f"Asset config overlay failed for {asset_class}: {e}")

    # Ensure explicit user config (args.config) wins over asset overlay defaults.
    # Note: `layers:` is handled separately as per-layer overrides.
    try:
        if isinstance(config_raw, dict):
            raw_top = {k: v for k, v in config_raw.items() if k != 'layers'}
            if raw_top:
                base_cfg = cfg.model_dump() if hasattr(cfg, 'model_dump') else (cfg.dict() if hasattr(cfg, 'dict') else {})
                _deep_merge(base_cfg, raw_top)
                cfg = TrainingConfig(**base_cfg)
    except Exception:
        pass
    mode_norm = str(mode or "train").strip().lower()
    if mode_norm not in {"train", "live"}:
        mode_norm = "train"

    # Disk safety: ensure minimal free space before heavy processing.
    try:
        req = 5.0
        try:
            req = float(getattr(cfg, 'packaging', {}).get('min_free_gb', req))
        except Exception:
            try:
                req = float(cfg.packaging.min_free_gb)
            except Exception:
                req = 5.0
    except Exception:
        req = 5.0
    if not ensure_disk_space(required_gb=req, workspace_root=str(REPO_ROOT)):
        print(f"Aborting run_sequence: nicht genügend freier Speicherplatz (benötigt {req} GB)")
        return {"error": "insufficient_disk_space"}

    layers_norm = None
    try:
        if layers:
            layers_norm = {str(x).strip().lower() for x in layers if str(x).strip()}
    except Exception:
        layers_norm = None

    # Strict TF cascade order.
    # Default operation: 1D + 1H + 30m only.
    # 5m/1m require explicit opt-in (CLI flag or config enable_micro_timeframes).
    enable_micro_timeframes = False
    try:
        if isinstance(config_raw, dict):
            enable_micro_timeframes = bool(config_raw.get("enable_micro_timeframes", False))
    except Exception:
        enable_micro_timeframes = False
    if include_5m or include_1m:
        enable_micro_timeframes = True

    require_parquet_for_tf: dict[str, bool] = {}
    try:
        cfg_data = getattr(cfg, "data", None)
        req_map = getattr(cfg_data, "require_parquet_for_tf", None) if cfg_data is not None else None
        if isinstance(req_map, dict):
            require_parquet_for_tf.update({str(k): bool(v) for k, v in req_map.items()})
    except Exception:
        pass
    try:
        if isinstance(config_raw, dict):
            raw_req = (config_raw.get("data") or {}).get("require_parquet_for_tf")
            if isinstance(raw_req, dict):
                require_parquet_for_tf.update({str(k): bool(v) for k, v in raw_req.items()})
    except Exception:
        pass

    def _require_parquet(tf: str) -> bool:
        key = str(tf)
        if key in require_parquet_for_tf:
            return bool(require_parquet_for_tf[key])
        return True

    tf_order = [
        {"tf": "1D", "name": "daily", "suffix": "", "hard_gate": True, "live_default": True, "live_role": "direction"},
        {"tf": "1H", "name": "struct_1h", "suffix": "1H", "hard_gate": True, "live_default": True, "live_role": "direction"},
        {"tf": "30m", "name": "struct_30m", "suffix": "30M", "hard_gate": True, "live_default": True, "live_role": "direction"},
    ]
    if enable_micro_timeframes:
        tf_order.append({"tf": "5m", "name": "entry_5m", "suffix": "5M", "hard_gate": True, "live_default": False, "live_role": "exit_only"})
        tf_order.append({"tf": "1m", "name": "exec_1m", "suffix": "1M", "hard_gate": True, "live_default": False, "live_role": "exit_only"})

    # Optional explicit layer selection:
    # Allowed only as a PREFIX of the cascade starting from 1D.
    if layers_norm:
        ordered_names = [str(l.get("name", "")).strip().lower() for l in tf_order]
        wanted = [n for n in ordered_names if n in layers_norm]
        # Validate prefix (no skipping parent layers).
        if wanted and wanted != ordered_names[: len(wanted)]:
            raise SystemExit(
                f"Invalid --layers: must be a prefix of {ordered_names}. "
                f"Refusing to run lower TFs without parent PASS."
            )
        tf_order = [l for l in tf_order if str(l.get("name", "")).strip().lower() in layers_norm]

    # Governance strictness: fail-closed always. --continue-on-fail is ignored.
    if continue_on_fail:
        print("WARNING: --continue-on-fail ignored (strict cascade is enforced)")
    stop_on_gate_fail = True

    # Cascade artifact root for this run.
    run_dir = Path(cfg.paths.reports_dir) / "cascade" / str(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    wants_30m = any(str(l.get("tf")).lower() == "30m" for l in tf_order)

    results = {}
    stopped_early = False
    stop_reason = None
    parent_tf = None
    parent_status = None
    attempted_30m = False
    for layer in tf_order:
        tf = str(layer.get("tf"))
        name = layer["name"]
        suffix = layer["suffix"]
        hard_gate = bool(layer.get("hard_gate", True))

        if tf.lower() == "30m":
            attempted_30m = True

        # Enforce parent PASS via pass_symbols_<TF>.txt as the ONLY allowed input.
        if parent_tf is not None:
            _repair_pass_list_from_decisions(run_dir, parent_tf)
            parent_pass_file = run_dir / f"pass_symbols_{parent_tf}.txt"
            parent_pass = _read_symbol_set(parent_pass_file)
            if _norm_symbol(symbol_base) not in parent_pass:
                if (not _require_parquet(parent_tf)) and _decision_allows_missing_skip(run_dir, parent_tf, symbol_base):
                    parent_status = "SKIP_MISSING_PARQUET"
                else:
                    reason = f"{parent_tf}_{parent_status or 'UNKNOWN'}"
                    print(f"Symbol={symbol_base} | TF={tf} | parent={parent_tf}:{parent_status} | SKIPPED | reason={reason}")
                    stopped_early = True
                    stop_reason = f"cascade_parent_not_pass:{parent_tf}"
                    break

        sym = find_symbol_variant(discovered, symbol_base, suffix)
        if not sym:
            print(f"Skipping {name}: parquet for {symbol_base} {suffix or '(daily)'} not found")
            allow_skip = (not _require_parquet(tf)) and str(tf).upper() != "1D"
            if allow_skip:
                results[name] = {"skipped": "missing_parquet", "base": symbol_base, "suffix": suffix}
                status = "SKIP_MISSING_PARQUET"
                decision = {
                    "symbol": _norm_symbol(symbol_base),
                    "timeframe": tf,
                    "status": status,
                    "gate_version": None,
                    "timestamp_utc": _utc_now_iso(),
                    "fail_reasons": ["missing_parquet_allowed"],
                    "required_outputs": {
                        "sharpe": None,
                        "max_drawdown": None,
                        "expectancy_net": None,
                        "stability_wf_std": None,
                        "cost_model": None,
                    },
                }
                _write_decision(run_dir, symbol_base, tf, decision)
                _update_tf_lists(run_dir, tf, symbol_base, status)
                continue
            results[name] = {"error": "missing_parquet", "base": symbol_base, "suffix": suffix}
            # Missing parquet is an ERR and stops cascade.
            status = "ERR"
            decision = {
                "symbol": _norm_symbol(symbol_base),
                "timeframe": tf,
                "status": status,
                "gate_version": None,
                "timestamp_utc": _utc_now_iso(),
                "fail_reasons": ["missing_parquet"],
                "required_outputs": {
                    "sharpe": None,
                    "max_drawdown": None,
                    "expectancy_net": None,
                    "stability_wf_std": None,
                    "cost_model": None,
                },
            }
            _write_decision(run_dir, symbol_base, tf, decision)
            _update_tf_lists(run_dir, tf, symbol_base, status)
            stopped_early = True
            stop_reason = f"missing_parquet:{name}"
            break
        # ensure state has asset_class for downstream pipeline inference
        try:
            state.update_symbol_state(sym, asset_class=asset_class)
            try:
                asset_cfg_file = Path('configs') / 'asset' / f"{asset_class}.yaml"
                if asset_cfg_file.exists():
                    state.update_symbol_state(sym, asset_config_overlay_path=str(asset_cfg_file))
            except Exception:
                pass
        except Exception:
            pass

        # report parquet basic info and validate sufficient real data
        pinfo = [d for d in discovered if d.symbol == sym][0]
        # check if symbol (or base) is already marked delisted in state; if so, quarantine & skip
        try:
            s_sym = state.get_symbol_state(sym) or {}
            s_base = state.get_symbol_state(symbol_base) or {}
            is_del = bool(int(s_sym.get('delisted') or 0)) if s_sym.get('delisted') is not None else False
            is_base_del = bool(int(s_base.get('delisted') or 0)) if s_base.get('delisted') is not None else False
            if is_del or is_base_del:
                # mark quarantine path and reason
                qroot = Path(cfg.paths.reports_dir) / 'quarantine' / symbol_base
                ts = datetime.utcnow().isoformat().replace(':', '-')
                qdir = qroot / ts
                qdir.mkdir(parents=True, exist_ok=True)
                try:
                    state.update_symbol_state(sym, artifact_quarantine_path=str(qdir), artifact_quarantine_reason='delisted', last_gate_result='DELISTED')
                except Exception:
                    pass
                send_telegram(cfg, f"OCTA: {sym} skipped (DELISTED) quarantine={str(qdir)}")
                print(f"Skipping {name} for {sym}: symbol is marked delisted (quarantined at {qdir})")
                results[name] = {"skipped": "delisted", "quarantine_path": str(qdir)}
                continue
        except Exception:
            pass
        # inspect parquet for delisting metadata before full load
        try:
            pinfo_ins = inspect_parquet(pinfo.path)
            if pinfo_ins.get('delisted'):
                # persist delisting state for base symbol and timeframe symbol
                try:
                    state.update_symbol_state(symbol_base, delisted=1, delisting_date=pinfo_ins.get('delisting_date'))
                    state.update_symbol_state(sym, delisted=1, delisting_date=pinfo_ins.get('delisting_date'))
                    print(f"Marked {symbol_base}/{sym} as delisted (date={pinfo_ins.get('delisting_date')})")
                    send_telegram(cfg, f"OCTA: Marked {symbol_base}/{sym} as DELISTED (date={pinfo_ins.get('delisting_date')})")
                except Exception:
                    pass
        except Exception:
            pass
        try:
            df_check = load_parquet(pinfo.path)
            rows = len(df_check)
            start = df_check.index.min() if rows else None
            end = df_check.index.max() if rows else None
        except Exception as e:
            print(f"Error reading parquet for {sym}: {e}")
            results[name] = {"error": "parquet_read_error", "message": str(e)}
            status = "ERR"
            decision = {
                "symbol": _norm_symbol(symbol_base),
                "timeframe": tf,
                "status": status,
                "gate_version": None,
                "timestamp_utc": _utc_now_iso(),
                "fail_reasons": [f"parquet_read_error: {str(e).splitlines()[0]}"] if str(e) else ["parquet_read_error"],
                "required_outputs": {
                    "sharpe": None,
                    "max_drawdown": None,
                    "expectancy_net": None,
                    "stability_wf_std": None,
                    "cost_model": None,
                },
            }
            _write_decision(run_dir, symbol_base, tf, decision)
            _update_tf_lists(run_dir, tf, symbol_base, status)
            stopped_early = True
            stop_reason = f"parquet_read_error:{name}"
            break

        splits_cfg = cfg.splits if hasattr(cfg, 'splits') else {}
        min_train = int(splits_cfg.get('min_train_size', 500))
        min_test = int(splits_cfg.get('min_test_size', 100))
        train_window = int(splits_cfg.get('train_window', 1000))
        test_window = int(splits_cfg.get('test_window', 200))
        # required bars conservatively
        required = max(min_train + min_test, train_window + test_window)
        print(f"Running layer {name} on symbol {sym} (rows={rows}, start={start}, end={end}, required>={required})")
        if rows < required and not force:
            print(f"Skipping {name} for {sym}: insufficient real data rows ({rows} < required {required})")
            results[name] = {"error": "insufficient_rows", "rows": rows, "required": required, "start": str(start), "end": str(end)}
            status = "ERR"
            decision = {
                "symbol": _norm_symbol(symbol_base),
                "timeframe": tf,
                "status": status,
                "gate_version": None,
                "timestamp_utc": _utc_now_iso(),
                "fail_reasons": [f"insufficient_rows: {rows} < {required}"],
                "required_outputs": {
                    "sharpe": None,
                    "max_drawdown": None,
                    "expectancy_net": None,
                    "stability_wf_std": None,
                    "cost_model": None,
                },
            }
            _write_decision(run_dir, symbol_base, tf, decision)
            _update_tf_lists(run_dir, tf, symbol_base, status)
            stopped_early = True
            stop_reason = f"insufficient_rows:{name}"
            break
        if rows < required and force:
            print(f"Force mode: continuing despite insufficient rows ({rows} < required {required})")

        # Build per-layer cfg overrides (if provided via raw config).
        layer_overrides = None
        try:
            if isinstance(config_raw, dict):
                layers_raw = config_raw.get('layers') or {}
                if isinstance(layers_raw, dict):
                    layer_overrides = layers_raw.get(name)
        except Exception:
            layer_overrides = None

        cfg_layer = _build_layer_cfg(cfg, layer_overrides)

        # Training != Live usage:
        # - In training mode we optionally save non-tradeable debug artifacts for 5m/1m even on gate FAIL.
        if mode_norm == "train" and name in {"entry_5m", "exec_1m"}:
            try:
                dbg_root = Path(cfg_layer.paths.pkl_dir) / "_research" / str(symbol_base) / str(run_id) / str(name)
                dbg_root.mkdir(parents=True, exist_ok=True)
                cfg_layer.packaging.save_debug_on_fail = True
                cfg_layer.packaging.debug_dir = str(dbg_root)
            except Exception:
                pass

        # Run the main trading pipeline for this timeframe.
        print(f"Symbol={symbol_base} | TF={tf} | parent={parent_tf}:{parent_status} | TRAINED")
        # Pass the discovered parquet path explicitly. This avoids relying on internal
        # symbol->filename reconstruction (which can fail on case-sensitive filesystems
        # when source files are lowercased like *_full_1day.parquet).
        res = train_evaluate_package(
            sym,
            cfg_layer,
            state,
            run_id,
            safe_mode=bool(gate_only),
            smoke_test=False,
            parquet_path=str(pinfo.path),
        )
        results[name] = res
        passed = bool(getattr(res, 'passed', False))
        print(f"Layer {name} result: passed={passed}")

        gate_version, reasons = _extract_gate_details(res)
        status = _decision_status(passed=passed, error=getattr(res, "error", None))
        if status == "ERR":
            # Prefer the pipeline error message if present.
            err = getattr(res, "error", None)
            if err:
                reasons = [str(err).splitlines()[0]]
            elif not reasons:
                reasons = ["pipeline_error"]

        decision = {
            "symbol": _norm_symbol(symbol_base),
            "timeframe": tf,
            "status": status,
            "gate_version": gate_version,
            "timestamp_utc": _utc_now_iso(),
            "fail_reasons": reasons if status != "PASS" else [],
            "required_outputs": {
                "sharpe": getattr(getattr(res, "metrics", None), "sharpe", None),
                "max_drawdown": getattr(getattr(res, "metrics", None), "max_drawdown", None),
                "expectancy_net": getattr(getattr(res, "metrics", None), "avg_net_trade_return", None),
                "stability_wf_std": getattr(getattr(res, "metrics", None), "sharpe_wf_std", None),
                "cost_model": {
                    "cost_bps": getattr(getattr(getattr(res, "metrics", None), "metadata", None), "cost_bps", None),
                    "spread_bps": getattr(getattr(getattr(res, "metrics", None), "metadata", None), "spread_bps", None),
                },
            },
        }
        _write_decision(run_dir, symbol_base, tf, decision)
        _update_tf_lists(run_dir, tf, symbol_base, status)
        if name == 'daily' and train_daily_regime and not gate_only:
            try:
                pinfo = [d for d in discovered if d.symbol == sym][0]
                df = load_parquet(pinfo.path)
                # Use the effective (layer) config features; build_features expects
                # a config-like object with nested `.features` dict.
                class _FeatSettings:
                    pass

                eff_settings = _FeatSettings()
                try:
                    eff_settings.features = cfg_layer.features if isinstance(cfg_layer.features, dict) else {}
                except Exception:
                    eff_settings.features = {}
                try:
                    for k, v in (eff_settings.features or {}).items():
                        if isinstance(k, str):
                            setattr(eff_settings, k, v)
                except Exception:
                    pass

                features_res = build_features(df, eff_settings, asset_class=asset_class or "unknown")
                X = features_res.X
                look = 20
                fwd_ret = (df['close'].shift(-look) / df['close']) - 1.0
                trend_label = (fwd_ret > 0.02).astype(int)
                trend_label = trend_label.reindex(X.index).fillna(0).astype(int)
                y_dict = {'y_cls_regime': trend_label}

                splits_cfg = cfg.splits if hasattr(cfg, 'splits') else {}
                try:
                    folds = walk_forward_splits(
                        X.index,
                        n_folds=int(splits_cfg.get('n_folds', 5)),
                        train_window=int(splits_cfg.get('train_window', 1000)),
                        test_window=int(splits_cfg.get('test_window', 200)),
                        step=int(splits_cfg.get('step', 200)),
                        purge_size=int(splits_cfg.get('purge_size', 10)),
                        embargo_size=int(splits_cfg.get('embargo_size', 5)),
                        min_train_size=int(splits_cfg.get('min_train_size', 500)),
                        min_test_size=int(splits_cfg.get('min_test_size', 100)),
                        expanding=bool(splits_cfg.get('expanding', True)),
                        min_folds_required=int(splits_cfg.get('min_folds_required', 1)),
                    )
                except ValueError as e:
                    print('walk_forward_splits failed (daily_regime):', e)
                    import numpy as _np
                    n = len(X.index)
                    folds = []
                    if n >= 10:
                        train_end = max(0, int(n * 0.7) - 1)
                        train_idx = _np.arange(0, train_end + 1)
                        val_idx = _np.arange(train_end + 1, n)
                        fold_meta = {
                            "train_range": (int(train_idx[0]) if train_idx.size else None, int(train_idx[-1]) if train_idx.size else None),
                            "val_range": (int(val_idx[0]) if val_idx.size else None, int(val_idx[-1]) if val_idx.size else None),
                            "train_size": int(train_idx.size),
                            "val_size": int(val_idx.size),
                        }
                        folds = [SplitFold(train_idx=train_idx, val_idx=val_idx, fold_meta=fold_meta)]
                        print('Fallback daily_regime: created single fold with sizes', fold_meta['train_size'], fold_meta['val_size'])
                    elif force and n >= 2:
                        train_end = 0
                        train_idx = _np.arange(0, train_end + 1)
                        val_idx = _np.arange(train_end + 1, n)
                        fold_meta = {
                            "train_range": (int(train_idx[0]) if train_idx.size else None, int(train_idx[-1]) if train_idx.size else None),
                            "val_range": (int(val_idx[0]) if val_idx.size else None, int(val_idx[-1]) if val_idx.size else None),
                            "train_size": int(train_idx.size),
                            "val_size": int(val_idx.size),
                        }
                        folds = [SplitFold(train_idx=train_idx, val_idx=val_idx, fold_meta=fold_meta)]
                        print('Force daily_regime: created minimal fold with sizes', fold_meta['train_size'], fold_meta['val_size'])

                profile = detect_device()
                if force:
                    TS = type('TS', (), {})
                    ts = TS()
                    ts.tuning = getattr(cfg, 'tuning', None)
                    ts.seed = getattr(cfg, 'seed', 42)
                    ts.scale_linear = getattr(cfg, 'scale_linear', True)
                    ts.splits = {
                        'n_folds': 1,
                        'train_window': 10,
                        'test_window': 2,
                        'step': 1,
                        'purge_size': 0,
                        'embargo_size': 0,
                        'min_train_size': 1,
                        'min_test_size': 1,
                        'min_folds_required': 1,
                    }
                    train_results = train_models(X, y_dict, folds, ts, profile, fast=False)
                else:
                    train_results = train_models(X, y_dict, folds, cfg, profile, fast=False)

                ok = False
                mean_auc = None
                if train_results:
                    best = train_results[0]
                    aucs = []
                    for fm in best.fold_metrics:
                        v = fm.metric.get('auc')
                        try:
                            aucs.append(float(v))
                        except Exception:
                            pass
                    if aucs:
                        mean_auc = float(np.nanmean(aucs))
                        ok = mean_auc >= 0.6
                results['daily_regime'] = {
                    "ok": ok,
                    "mean_auc": mean_auc,
                    "model": train_results[0].model_name if train_results else None,
                }
                print(f"Layer daily_regime quality OK={ok} mean_auc={mean_auc}")
            except Exception as e:
                results['daily_regime'] = {"error": str(e)}
                print(f"daily_regime failed: {e}")

        # Strict cascade: stop immediately on any FAIL/ERR.
        if stop_on_gate_fail and hard_gate and status != "PASS":
            stopped_early = True
            stop_reason = f"cascade_stop:{tf}:{status}"
            break

        parent_tf = tf
        parent_status = status

    # Write a compact run report.
    try:
        reports_dir = Path(cfg.paths.reports_dir)
        reports_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).isoformat().replace(":", "-")
        out_path = reports_dir / f"{symbol_base}_multitf_{run_id}_{ts}.json"

        serializable = {
            "symbol_base": symbol_base,
            "run_id": run_id,
            "utc_ts": ts,
            "raw_dir": str(cfg.paths.raw_dir),
            "asset_class": asset_class,
            "mode": mode_norm,
            "live_policy": {
                "notes": "Deterministic matrix: cases A-D. Higher TFs dominate. 5m/1m are exit-only; 1m has 0% weight in live.",
            },
            "cascade_artifacts_dir": str(run_dir),
            "stopped_early": stopped_early,
            "stop_reason": stop_reason,
            "layers": {},
        }
        for k, v in results.items():
            if isinstance(v, dict):
                serializable["layers"][k] = v
            else:
                rr = _pipeline_result_to_dict(v)
                if isinstance(rr, dict) and k in {"daily", "struct_1h", "struct_30m", "entry_5m", "exec_1m"}:
                    # annotate layer role (training vs live usage)
                    meta = {}
                    for l in tf_order:
                        if l.get("name") == k:
                            meta = {
                                "live_default": bool(l.get("live_default", False)),
                                "live_role": str(l.get("live_role", "unknown")),
                            }
                            break
                    rr["layer_meta"] = meta
                serializable["layers"][k] = rr

        # Compute deterministic live-release decision from gate+metrics.
        try:
            by_tf = {}
            layer_map = {
                "1D": serializable["layers"].get("daily"),
                "1H": serializable["layers"].get("struct_1h"),
                "30m": serializable["layers"].get("struct_30m"),
                "5m": serializable["layers"].get("entry_5m"),
                "1m": serializable["layers"].get("exec_1m"),
            }
            for tfk, ldict in layer_map.items():
                if isinstance(ldict, dict):
                    by_tf[tfk] = outcome_from_pipeline_dict(tfk, ldict)
            decision = determine_live_release(by_tf)
            serializable["live_release"] = {
                "mode": decision.mode.value,
                "position_size_cap": decision.position_size_cap,
                "intraday_allowed": decision.intraday_allowed,
                "reentries_allowed": decision.reentries_allowed,
                "entries_allowed": decision.entries_allowed,
                "min_holding_days": decision.min_holding_days,
                "allow_scale_in": decision.allow_scale_in,
                "direction_sources": list(decision.direction_sources),
                "timing_sources": list(decision.timing_sources),
                "exit_sources": list(decision.exit_sources),
                "micro_layer_weight_5m": decision.micro_layer_weight_5m,
                "micro_layer_weight_1m": decision.micro_layer_weight_1m,
                "reason": decision.reason,
            }
        except Exception as e:
            serializable["live_release"] = {"error": str(e)}

        out_path.write_text(json.dumps(serializable, indent=2, default=str))
        print(f"Wrote multi-timeframe report: {out_path}")
    except Exception as e:
        print(f"Failed to write multi-timeframe report: {e}")

    # --- HF 30m eligibility + mandatory NDJSON ---
    try:
        # Ensure there is always a 30m decision artifact only when 30m was actually
        # attempted in this run. Strict cascade semantics: if we stopped earlier
        # (e.g., 1H FAIL), we must not fabricate a 30m decision artifact.
        tf_30m = "30m"
        if not attempted_30m:
            return results
        sym_norm = _norm_symbol(symbol_base)
        decision_30m_path = run_dir / sym_norm / tf_30m / "decision.json"
        decision_30m = None
        if decision_30m_path.exists():
            try:
                decision_30m = json.loads(decision_30m_path.read_text())
            except Exception:
                decision_30m = None

        # Derive fast eligibility flags from parquet metadata (no full load).
        daily_sym = find_symbol_variant(discovered, symbol_base, "")
        m30_sym = find_symbol_variant(discovered, symbol_base, "30M")
        daily_path = None
        m30_path = None
        try:
            if daily_sym:
                daily_path = next((d.path for d in discovered if d.symbol == daily_sym), None)
            if m30_sym:
                m30_path = next((d.path for d in discovered if d.symbol == m30_sym), None)
        except Exception:
            daily_path = None
            m30_path = None

        daily_rows = _parquet_row_count(Path(daily_path)) if daily_path else None
        m30_rows = _parquet_row_count(Path(m30_path)) if m30_path else None

        # Eligibility (A/B/C - minimal conservative interpretation):
        # A) daily exists and has >=600 rows
        # B) 30m exists and has enough bars for configured splits
        # C) basic structure sanity (has close column) and not delisted (metadata)
        splits_cfg = cfg.splits if hasattr(cfg, 'splits') else {}
        min_train = int(splits_cfg.get('min_train_size', 500))
        min_test = int(splits_cfg.get('min_test_size', 100))
        train_window = int(splits_cfg.get('train_window', 1000))
        test_window = int(splits_cfg.get('test_window', 200))
        required_intraday = max(min_train + min_test, train_window + test_window)

        daily_ok = bool(daily_rows is not None and daily_rows >= 600)
        intraday_ok = bool(m30_rows is not None and m30_rows >= required_intraday)

        delisted = False
        delisting_date = None
        cols_ok = True
        if daily_path:
            d_del, d_dt = _parquet_delisted_fast(Path(daily_path))
            delisted = delisted or bool(d_del)
            delisting_date = delisting_date or d_dt
            cols = _parquet_columns(Path(daily_path))
            if isinstance(cols, list) and cols:
                cols_ok = cols_ok and ("close" in {c.lower() for c in cols})
        if m30_path:
            i_del, i_dt = _parquet_delisted_fast(Path(m30_path))
            delisted = delisted or bool(i_del)
            delisting_date = delisting_date or i_dt
            cols = _parquet_columns(Path(m30_path))
            if isinstance(cols, list) and cols:
                cols_ok = cols_ok and ("close" in {c.lower() for c in cols})

        structure_ok = bool((not delisted) and cols_ok)

        eligibility_flags = {
            "daily_ok": daily_ok,
            "intraday_ok": intraday_ok,
            "structure_ok": structure_ok,
            "daily_rows": daily_rows,
            "intraday_rows_30m": m30_rows,
            "required_intraday_rows_30m": required_intraday,
            "delisted": delisted,
            "delisting_date": delisting_date,
        }

        if wants_30m and not decision_30m_path.exists():
            # If we never evaluated 30m, emit a deterministic SKIP/ERROR decision.
            skip_reason = None
            synthetic_status = None

            # Treat upstream gate/parent failures + data insufficiency as ineligible (SKIP).
            if stop_reason and (
                stop_reason.startswith("cascade_parent_not_pass")
                or stop_reason.startswith("cascade_stop")
                or stop_reason.startswith("missing_parquet")
                or stop_reason.startswith("insufficient_rows")
            ):
                synthetic_status = "SKIP_30M_NOT_ELIGIBLE"
                skip_reason = stop_reason
            elif stop_reason and stop_reason.startswith("parquet_read_error"):
                synthetic_status = "ERR"
            else:
                # Default: if any eligibility flag fails, SKIP; otherwise ERR.
                if not (daily_ok and intraday_ok and structure_ok):
                    synthetic_status = "SKIP_30M_NOT_ELIGIBLE"
                    skip_reason = "eligibility_failed"
                else:
                    synthetic_status = "ERR"

            decision_30m = {
                "symbol": sym_norm,
                "timeframe": tf_30m,
                "status": synthetic_status,
                "gate_version": None,
                "timestamp_utc": _utc_now_iso(),
                "fail_reasons": [] if synthetic_status.startswith("SKIP") else ([str(stop_reason)] if stop_reason else ["missing_30m_decision"]),
                "skip_reason": skip_reason,
                "eligibility_flags": eligibility_flags,
                "required_outputs": {
                    "sharpe": None,
                    "max_drawdown": None,
                    "expectancy_net": None,
                    "stability_wf_std": None,
                    "cost_model": None,
                },
            }
            _write_decision(run_dir, symbol_base, tf_30m, decision_30m)
            _update_tf_lists(run_dir, tf_30m, symbol_base, synthetic_status)

        # Load 30m decision (again) to drive NDJSON status.
        if decision_30m is None and decision_30m_path.exists():
            try:
                decision_30m = json.loads(decision_30m_path.read_text())
            except Exception:
                decision_30m = None

        # Prepare NDJSON record (exactly one line per symbol/run).
        raw_status = None
        fail_reasons = []
        skip_reason = None
        gate_version = None
        if isinstance(decision_30m, dict):
            raw_status = decision_30m.get("status")
            gate_version = decision_30m.get("gate_version")
            fail_reasons = [str(r) for r in (decision_30m.get("fail_reasons") or [])]
            skip_reason = decision_30m.get("skip_reason")
            # Prefer embedded eligibility flags if present (synthetic decision).
            if isinstance(decision_30m.get("eligibility_flags"), dict):
                eligibility_flags = decision_30m["eligibility_flags"]

        hf_status = _hf30m_status_from_decision(str(raw_status or ""))

        hf_metrics = None
        # Only include metrics when evaluated (PASS/FAIL at 30m).
        if hf_status in {"PASS", "FAIL_HF_METRICS"}:
            try:
                r30 = results.get("struct_30m")
                if r30 is not None and not isinstance(r30, dict):
                    rr = _pipeline_result_to_dict(r30)
                    if isinstance(rr, dict):
                        hf_metrics = {
                            "gate_version": gate_version,
                            "n_trades": ((rr.get("metrics") or {}).get("n_trades") if isinstance(rr.get("metrics"), dict) else None),
                            "sharpe": ((rr.get("metrics") or {}).get("sharpe") if isinstance(rr.get("metrics"), dict) else None),
                            "sortino": ((rr.get("metrics") or {}).get("sortino") if isinstance(rr.get("metrics"), dict) else None),
                            "sharpe_wf_std": ((rr.get("metrics") or {}).get("sharpe_wf_std") if isinstance(rr.get("metrics"), dict) else None),
                            "max_drawdown": ((rr.get("metrics") or {}).get("max_drawdown") if isinstance(rr.get("metrics"), dict) else None),
                            "cagr": ((rr.get("metrics") or {}).get("cagr") if isinstance(rr.get("metrics"), dict) else None),
                            "avg_net_trade_return": ((rr.get("metrics") or {}).get("avg_net_trade_return") if isinstance(rr.get("metrics"), dict) else None),
                            "metadata": ((rr.get("metrics") or {}).get("metadata") if isinstance(rr.get("metrics"), dict) else None),
                        }
            except Exception:
                hf_metrics = None

        ndjson_record = {
            "symbol": sym_norm,
            "asset_profile": str(asset_class or "unknown"),
            "timeframe": tf_30m,
            "status": hf_status,
            "eligibility_flags": eligibility_flags,
        }
        if hf_metrics is not None:
            ndjson_record["hf_metrics"] = hf_metrics
        if hf_status == "SKIP_30M_NOT_ELIGIBLE":
            ndjson_record["skip_reason"] = str(skip_reason or stop_reason or "not_eligible")
        elif hf_status == "FAIL_HF_METRICS":
            ndjson_record["fail_reasons"] = fail_reasons
        elif hf_status == "ERROR":
            ndjson_record["fail_reasons"] = fail_reasons or ([str(stop_reason)] if stop_reason else ["error"])

        hf_ndjson_path = Path(cfg.paths.reports_dir) / "training_30m" / str(run_id) / "hf_30m.ndjson"
        _atomic_append_ndjson(hf_ndjson_path, ndjson_record)
    except Exception as e:
        print(f"Failed to emit HF 30m NDJSON: {e}")

    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', required=True, help='Base symbol name (daily parquet must be SYMBOL)')
    parser.add_argument('--force', action='store_true', help='Force training even if rows < required or folds insufficient')
    parser.add_argument('--continue-on-fail', action='store_true', help='IGNORED (strict cascade enforced; kept for backward CLI compatibility)')
    parser.add_argument('--include-5m', action='store_true', help='Include 5m layer in the cascade (research/exit-only)')
    parser.add_argument('--include-1m', action='store_true', help='Include 1m layer in the cascade (research/exit-only)')
    parser.add_argument('--no-daily-regime', action='store_true', help='Disable daily_regime side-model')
    parser.add_argument('--gate-only', action='store_true', help='Only evaluate gates (safe mode); do not run daily_regime training')
    parser.add_argument('--mode', choices=['train', 'live'], default='train', help='Execution mode label (strict cascade is enforced in all modes).')
    parser.add_argument('--layers', default=None, help='Comma-separated layer names to run (e.g. "daily,struct_1h"). If set, overrides default TF selection (must be a prefix).')
    parser.add_argument('--run-id', default='multi_tf_run', help='Run id')
    parser.add_argument('--config', default=None, help='Path to a training config YAML (optional)')
    args = parser.parse_args()

    cfg_raw = None
    if getattr(args, 'config', None) and yaml:
        try:
            cfg_raw = yaml.safe_load(Path(args.config).read_text()) or {}
        except Exception:
            cfg_raw = None

    cfg = load_config(args.config) if getattr(args, 'config', None) else load_config()

    mode_norm = str(getattr(args, 'mode', 'train')).strip().lower()
    include_5m = bool(getattr(args, 'include_5m', False))
    include_1m = bool(getattr(args, 'include_1m', False))
    continue_on_fail = bool(getattr(args, 'continue_on_fail', False))
    layers_arg = getattr(args, 'layers', None)
    layers = None
    try:
        if layers_arg:
            layers = [s.strip() for s in str(layers_arg).split(',') if s.strip()]
    except Exception:
        layers = None

    # Defaults are governance-safe: 5m/1m disabled unless explicitly enabled.

    state = StateRegistry(cfg.paths.state_dir)
    out = run_sequence(
        args.symbol,
        cfg,
        state,
        run_id=args.run_id,
        force=getattr(args, 'force', False),
        include_5m=include_5m,
        include_1m=include_1m,
        continue_on_fail=continue_on_fail,
        train_daily_regime=not bool(getattr(args, 'no_daily_regime', False)),
        gate_only=bool(getattr(args, 'gate_only', False)),
        mode=mode_norm,
        config_raw=cfg_raw,
        layers=layers,
    )
    for k, v in out.items():
        if v is None:
            print(k, '->', None)
            continue
        # list of TrainResult for daily
        if isinstance(v, list):
            if len(v) == 0:
                print(k, '->', 'no train results')
            else:
                best = v[0]
                # collect fold aucs if present
                aucs = [fm.metric.get('auc') for fm in best.fold_metrics]
                print(k, '->', f"train_results model={best.model_name} task={best.task} horizon={best.horizon} mean_auc={np.nanmean(aucs) if aucs else None}")
            continue
        # PipelineResult-like
        try:
            print(k, '->', (v.passed, getattr(v, 'metrics', None)))
        except Exception:
            print(k, '->', str(v))
