#!/usr/bin/env python3
"""Batch gate check for all index parquet files (1D).

Goal: quickly validate that gates/metrics behave sensibly across the whole index universe,
without packaging artifacts.

- Discovers all *.parquet under cfg.paths.raw_dir (excluding PKL)
- Prefilters to asset_class == 'index' using fast Parquet schema inspection
- Forces 1D resampling during load (real-data aggregation; no simulation)
- Runs train_evaluate_package in safe_mode=True (no artifact writes)
- Writes a JSON report with per-symbol results + aggregate stats

Usage:
  python3 scripts/batch_gate_check_indices_1d.py --config configs/tmp_train_djr_multitf_hf.yaml --workers 4
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import warnings
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Allow running as a script from the repo root without installing the package
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from octa_training.core.asset_class import infer_asset_class
from octa_training.core.config import TrainingConfig, load_config
from octa_training.core.io_parquet import ParquetFileInfo, sanitize_symbol
from octa_training.core.pipeline import train_evaluate_package
from octa_training.core.state import StateRegistry


def _json_default(o: Any):
    """Best-effort JSON encoding for sweep reports.

    Some dependencies (pydantic/numpy/pyarrow) can leak scalar wrapper types
    that are not directly serializable by json.
    """
    try:
        # numpy/pandas scalar
        item = getattr(o, "item", None)
        if callable(item):
            return item()
    except Exception:
        pass

    try:
        # pydantic v2 core types can have surprising class names
        if getattr(o, "__class__", None) is not None and o.__class__.__name__ == "bool":
            return bool(o)
    except Exception:
        pass

    try:
        from pathlib import Path as _Path

        if isinstance(o, _Path):
            return str(o)
    except Exception:
        pass

    try:
        import pandas as _pd

        if isinstance(o, (_pd.Timestamp, _pd.Timedelta)):
            return o.isoformat()
    except Exception:
        pass

    return str(o)


def _now_tag() -> str:
    return datetime.now(timezone.utc).isoformat().replace(":", "-")


def _normalize_asset_class(label: Optional[str]) -> str:
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


def _parquet_columns_fast(path: Path) -> List[str]:
    try:
        import pyarrow.parquet as pq

        schema = pq.read_schema(str(path))
        return [str(n).lower() for n in schema.names]
    except Exception:
        # fallback: no fast path
        return []


def _base_symbol_key(sym: str) -> str:
    """Heuristic: collapse common timeframe suffix variants to a base key.

    This is intentionally simple to reduce duplicate work (e.g. DJR_1H, DJR_30M, DJR_5M).
    """
    s = str(sym or "").upper().strip()
    suffixes = [
        "_FULL_DAILY",
        "_FULL_1DAY",
        "_DAILY",
        "_1DAY",
        "_1D",
        "_FULL_1HOUR",
        "_FULL_1H",
        "_1HOUR",
        "_1HR",
        "_1H",
        "_60MIN",
        "_60M",
        "_FULL_30MIN",
        "_30MIN",
        "_30M",
        "_FULL_5MIN",
        "_5MIN",
        "_5M",
        "_FULL_1MIN",
        "_1MIN",
        "_1M",
    ]
    for suf in suffixes:
        if s.endswith(suf):
            return s[: -len(suf)]
    return s


def _is_intraday_named(sym: str) -> bool:
    s = str(sym or "").upper()
    return any(tok in s for tok in ("_1MIN", "_1M", "_5MIN", "_5M", "_30MIN", "_30M", "_1H", "_1HR", "_1HOUR", "_60MIN", "_60M"))


def _is_daily_named(sym: str) -> bool:
    s = str(sym or "").upper()
    return any(tok in s for tok in ("_1DAY", "_1D", "_DAILY", "_FULL_1DAY", "_FULL_DAILY")) or (not _is_intraday_named(s))


def _infer_time_column_name(cols: List[str]) -> Optional[str]:
    cols_l = [c.lower() for c in (cols or [])]
    for cand in ["timestamp", "datetime", "date", "time"]:
        if cand in cols_l:
            return cols[cols_l.index(cand)]
    return None


def _seems_daily_fast(path: Path, cols: List[str]) -> Optional[bool]:
    """Best-effort check if parquet is daily-ish, without loading full OHLC.

    Returns:
      True/False when determinable, else None.
    """
    try:
        import numpy as np
        import pandas as pd
        import pyarrow.parquet as pq

        tcol = _infer_time_column_name(cols)
        if not tcol:
            return None
        pf = pq.ParquetFile(str(path))
        if pf.num_row_groups < 1:
            return None
        # read first rowgroup timestamps only
        tab = pf.read_row_group(0, columns=[tcol])
        s = tab.column(0).to_pandas()
        idx = pd.to_datetime(s, utc=True, errors="coerce").dropna()
        if len(idx) < 3:
            return None
        idx = idx.sort_values().unique()
        if len(idx) < 3:
            return None
        # median delta in seconds
        deltas = np.diff(idx.astype("datetime64[ns]").astype("int64"))
        med_ns = float(np.median(deltas))
        if med_ns <= 0:
            return None
        sec = med_ns / 1e9
        return bool(sec >= 20 * 3600)
    except Exception:
        return None


def _metric_get(m: Any, key: str):
    if m is None:
        return None
    try:
        return getattr(m, key)
    except Exception:
        try:
            if isinstance(m, dict):
                return m.get(key)
        except Exception:
            return None
    return None


def _result_to_dict(res) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "symbol": getattr(res, "symbol", None),
        "run_id": getattr(res, "run_id", None),
        "passed": bool(getattr(res, "passed", False)),
        "error": getattr(res, "error", None),
        "metrics": {},
        "gate": None,
    }

    metrics = getattr(res, "metrics", None)
    out["metrics"] = {
        "n_trades": _metric_get(metrics, "n_trades"),
        "sharpe": _metric_get(metrics, "sharpe"),
        "sortino": _metric_get(metrics, "sortino"),
        "max_drawdown": _metric_get(metrics, "max_drawdown"),
        "profit_factor": _metric_get(metrics, "profit_factor"),
        "cagr": _metric_get(metrics, "cagr"),
        "cvar_99": _metric_get(metrics, "cvar_99"),
        "cvar_99_sigma": _metric_get(metrics, "cvar_99_sigma"),
        "turnover_per_day": _metric_get(metrics, "turnover_per_day"),
        "avg_gross_exposure": _metric_get(metrics, "avg_gross_exposure"),
    }

    gate = getattr(res, "gate_result", None)
    try:
        if gate is not None:
            if hasattr(gate, "model_dump"):
                out["gate"] = gate.model_dump()
            elif hasattr(gate, "dict"):
                out["gate"] = gate.dict()
            else:
                out["gate"] = {
                    "passed": getattr(gate, "passed", None),
                    "reasons": getattr(gate, "reasons", None),
                    "passed_checks": getattr(gate, "passed_checks", None),
                    "robustness": getattr(gate, "robustness", None),
                }
    except Exception:
        out["gate"] = None

    return out


def _apply_batch_overrides(cfg: TrainingConfig) -> TrainingConfig:
    # Force "fast" institutional gate sweep:
    # - no Optuna tuning
    # - single simple model
    # - 1 horizon only
    # - resample everything to 1D
    # - avoid "recent_pass" skipping so we always re-evaluate
    raw = cfg.model_dump() if hasattr(cfg, "model_dump") else cfg.dict()

    # tuning off
    raw.setdefault("tuning", {})
    raw["tuning"]["enabled"] = False
    raw["tuning"]["models_order"] = ["logreg"]

    # horizons minimal
    raw.setdefault("features", {})
    raw["features"]["horizons"] = [1]

    # 1D resampling
    raw.setdefault("parquet", {})
    raw["parquet"]["resample_enabled"] = True
    raw["parquet"]["resample_bar_size"] = "1D"

    # always re-evaluate
    raw.setdefault("retrain", {})
    raw["retrain"]["skip_window_days"] = 0

    # slightly reduce CV cost
    raw.setdefault("splits", {})
    # take most recent folds for speed and relevance
    raw["splits"]["n_folds"] = -3

    # Speed-only overrides for robustness intensity (keeps same thresholds).
    raw.setdefault("gates", {})
    raw["gates"].setdefault("hard_kill_switches", {})
    # Disable nondeterminism check for quick sweeps; enable in production runs.
    raw["gates"]["hard_kill_switches"].setdefault("nondeterminism_check", False)
    # Reduce permutation test cost for sweeps.
    raw["gates"].setdefault("robustness", {})
    raw["gates"]["robustness"].setdefault("robustness_permutation_max_folds", 2)
    raw["gates"]["robustness"].setdefault("robustness_permutation_n_shuffles", 5)

    return TrainingConfig(**raw)


def main() -> int:
    # Batch sweeps intentionally hit early-window warmup regions for rolling features/targets.
    # Pandas may emit noisy RuntimeWarnings for all-NaN rolling windows; suppress them here
    # so sweep output focuses on actionable gate failures and data errors.
    warnings.filterwarnings(
        "ignore",
        message="All-NaN slice encountered",
        category=RuntimeWarning,
        module=r"pandas\.core\.window\.rolling",
    )

    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None, help="Training config YAML")
    ap.add_argument("--raw-dir", default=None, help="Override raw data directory for discovery (e.g. raw/Indices_parquet)")
    ap.add_argument("--out", default=None, help="Output report path (json)")
    ap.add_argument("--max-symbols", type=int, default=0, help="Limit number of symbols (0 = all)")
    ap.add_argument(
        "--only-symbol",
        default=None,
        help="Evaluate only this symbol (after sanitization/base-dedupe). Useful for targeted reruns.",
    )
    ap.add_argument(
        "--no-fast-overrides",
        action="store_true",
        help="Disable batch 'fast overrides' (use config as-is). Useful to reproduce prior PASS behavior.",
    )
    ap.add_argument("--include-intraday", action="store_true", help="Include intraday parquet variants (1m/5m/30m/1h). Slower.")
    ap.add_argument("--skip-errors", action="store_true", help="Skip symbols that error")
    ap.add_argument(
        "--exclude-symbols-file",
        default=None,
        help="Path to newline-delimited list of symbols to exclude (e.g. *_exclude_symbols.txt)",
    )
    ap.add_argument("--cascade", action="store_true", help="If set, run per-symbol multi-TF orchestrator for 1D PASS symbols")
    ap.add_argument("--cascade-layers", default=None, help='Comma-separated layers to pass to train_multiframe_symbol (e.g. "daily,struct_1h,struct_30m")')
    ap.add_argument("--cascade-include-5m", action="store_true", help="When cascading, include 5m layer")
    ap.add_argument("--cascade-include-1m", action="store_true", help="When cascading, include 1m layer")
    ap.add_argument("--cascade-gate-only", action="store_true", help="When cascading, only evaluate gates (safe mode), do not run full daily_regime training")
    args = ap.parse_args()

    cfg = load_config(args.config)
    applied_overrides = False
    if not getattr(args, "no_fast_overrides", False):
        cfg = _apply_batch_overrides(cfg)
        applied_overrides = True

    state = StateRegistry(str(cfg.paths.state_dir))

    # Discover universe: prefer explicit --raw-dir; otherwise auto-detect common indices folders;
    # finally fall back to cfg.paths.raw_dir.
    raw_dir = None
    if args.raw_dir:
        raw_dir = Path(args.raw_dir)
    else:
        candidates = [
            Path("raw") / "Indices_parquet",
            Path("raw") / "INDICES_PARQUET",
            Path("raw") / "Indices",
            Path("raw") / "INDICES",
        ]
        for c in candidates:
            if c.exists() and c.is_dir():
                raw_dir = c
                break
    if raw_dir is None:
        raw_dir = Path(cfg.paths.raw_dir)

    # Ensure pipeline uses the same raw_dir we discovered from.
    try:
        cfg.paths.raw_dir = raw_dir
    except Exception:
        # fallback: rebuild cfg with updated raw_dir
        raw_cfg = cfg.model_dump() if hasattr(cfg, "model_dump") else cfg.dict()
        raw_cfg.setdefault("paths", {})
        raw_cfg["paths"]["raw_dir"] = str(raw_dir)
        cfg = TrainingConfig(**raw_cfg)

    # Fast discovery without hashing: map sanitized stem -> path.
    # This is significantly faster than discover_parquets() for large universes.
    discovered: List[ParquetFileInfo] = []
    seen: Dict[str, ParquetFileInfo] = {}
    for p in Path(raw_dir).rglob("*.parquet"):
        try:
            stat = p.stat()
        except FileNotFoundError:
            continue
        sym = sanitize_symbol(p.stem)
        info = ParquetFileInfo(symbol=sym, path=p, mtime=stat.st_mtime, size=stat.st_size, sha256=None)
        # if duplicates exist, keep the newest
        cur = seen.get(sym)
        if cur is None or info.mtime > cur.mtime:
            seen[sym] = info
    discovered = list(seen.values())
    print(f"Discovered parquets: {len(discovered)} under {raw_dir}")

    # Prefilter to asset_class == index using fast schema columns.
    # Step 1: infer asset class quickly.
    idx_candidates: List[Tuple[ParquetFileInfo, List[str]]] = []
    for pinfo in discovered:
        if (not args.include_intraday) and _is_intraday_named(pinfo.symbol):
            continue
        cols = _parquet_columns_fast(pinfo.path)
        try:
            ac = infer_asset_class(pinfo.symbol, str(pinfo.path), cols, cfg)
        except Exception:
            ac = "unknown"
        if _normalize_asset_class(ac) == "index":
            idx_candidates.append((pinfo, cols))

    print(f"Index candidates (post intraday filter): {len(idx_candidates)}")

    # Step 2: prefer daily-ish originals (still resampled to 1D during load).
    dailyish: List[ParquetFileInfo] = []
    for pinfo, cols in idx_candidates:
        d = _seems_daily_fast(pinfo.path, cols)
        if d is None or d is True:
            dailyish.append(pinfo)

    print(f"Daily-ish candidates: {len(dailyish)}")

    # Step 3: dedupe by base key to avoid repeated work across timeframe variants.
    by_base: Dict[str, ParquetFileInfo] = {}
    for pinfo in dailyish:
        k = _base_symbol_key(pinfo.symbol)
        cur = by_base.get(k)
        if cur is None:
            by_base[k] = pinfo
            continue
        # prefer shorter symbol name (usually base daily) and larger file as a proxy for history
        if len(pinfo.symbol) < len(cur.symbol):
            by_base[k] = pinfo
        elif len(pinfo.symbol) == len(cur.symbol) and pinfo.size > cur.size:
            by_base[k] = pinfo

    index_infos_all = sorted(by_base.values(), key=lambda x: x.symbol)
    print(f"Selected unique index bases: {len(index_infos_all)}")

    # Optional explicit exclusions (fail-closed): remove known-bad symbols from evaluation.
    excluded_syms: set[str] = set()
    if args.exclude_symbols_file:
        p = Path(str(args.exclude_symbols_file))
        if not p.exists():
            raise SystemExit(f"exclude-symbols-file not found: {p}")
        for line in p.read_text().splitlines():
            s = sanitize_symbol(line.strip())
            if s:
                excluded_syms.add(s)
        if excluded_syms:
            print(f"Excluding {len(excluded_syms)} symbols from exclude list")

    # Apply optional exclusions and truncation for smoke-testing.
    if excluded_syms:
        index_infos = [pinfo for pinfo in index_infos_all if sanitize_symbol(pinfo.symbol) not in excluded_syms]
    else:
        index_infos = index_infos_all

    # Optional single-symbol filter (post base-dedupe). Accepts either the full symbol
    # or its base key; comparison is sanitized/uppercased for robustness.
    if getattr(args, "only_symbol", None):
        target = sanitize_symbol(str(args.only_symbol)).upper()
        if not target:
            raise SystemExit("--only-symbol provided but empty after sanitization")
        filtered: List[ParquetFileInfo] = []
        for pinfo in index_infos:
            sym = sanitize_symbol(pinfo.symbol).upper()
            base = sanitize_symbol(_base_symbol_key(pinfo.symbol)).upper()
            if sym == target or base == target:
                filtered.append(pinfo)
        if not filtered:
            universe_preview = ", ".join([p.symbol for p in index_infos[:20]])
            raise SystemExit(
                f"--only-symbol '{args.only_symbol}' not found after discovery/dedupe. "
                f"Universe sample: {universe_preview}"
            )
        index_infos = filtered
        print(f"Filtered to only-symbol={target}: {len(index_infos)} file(s)")

    if args.max_symbols and args.max_symbols > 0:
        index_infos = index_infos[: args.max_symbols]

    run_id = f"index_gate_1d_{_now_tag()}"

    results: List[Dict[str, Any]] = []
    reason_counter: Counter[str] = Counter()
    passed = 0
    failed = 0
    errored = 0

    for i, pinfo in enumerate(index_infos, 1):
        print(f"[{i}/{len(index_infos)}] {pinfo.symbol} -> train+gate (1D)")
        res = train_evaluate_package(pinfo.symbol, cfg, state, run_id=run_id, safe_mode=True, parquet_path=str(pinfo.path))
        d = _result_to_dict(res)
        results.append(d)

        # Accounting: partition outcomes into exactly one of {passed, failed, errored}.
        if d.get("error"):
            errored += 1
            continue

        if d.get("passed"):
            passed += 1
            # Optionally run the per-symbol multi-TF orchestrator for PASS symbols.
            if getattr(args, 'cascade', False):
                try:
                    base_sym = _base_symbol_key(pinfo.symbol)
                    cmd = [sys.executable, str(REPO_ROOT / 'scripts' / 'train_multiframe_symbol.py'), '--symbol', base_sym, '--run-id', run_id]
                    if getattr(args, 'config', None):
                        cmd += ['--config', args.config]
                    if getattr(args, 'cascade_layers', None):
                        cmd += ['--layers', args.cascade_layers]
                    if getattr(args, 'cascade_include_5m', False):
                        cmd += ['--include-5m']
                    if getattr(args, 'cascade_include_1m', False):
                        cmd += ['--include-1m']
                    if getattr(args, 'cascade_gate_only', False):
                        cmd += ['--gate-only']
                    print(f"Invoking cascade for {base_sym}: {' '.join(cmd)}")
                    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
                    print(f"Cascade exit={proc.returncode} stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
                except Exception as e:
                    print(f"Failed to invoke cascade for {pinfo.symbol}: {e}")
            continue

        failed += 1

        g = d.get("gate") or {}
        reasons = g.get("reasons") if isinstance(g, dict) else None
        if isinstance(reasons, list):
            for r in reasons:
                if r:
                    reason_counter[str(r)] += 1

    out_path = Path(args.out) if args.out else (Path(cfg.paths.reports_dir) / f"index_gate_sweep_1d_{_now_tag()}.json")
    payload = {
        "run_id": run_id,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "config": {
            "config_path": args.config,
            "raw_dir": str(raw_dir),
            "exclude_symbols_file": str(args.exclude_symbols_file) if args.exclude_symbols_file else None,
            "only_symbol": str(args.only_symbol) if args.only_symbol else None,
            "no_fast_overrides": bool(getattr(args, "no_fast_overrides", False)),
            "overrides": {
                "applied": applied_overrides,
                "tuning_enabled": False,
                "models_order": ["logreg"],
                "horizons": [1],
                "resample": True,
                "resample_bar_size": "1D",
                "n_folds": -3,
            },
        },
        "universe": {
            "discovered_parquets": len(discovered),
            "index_parquets": len(index_infos_all),
            "index_parquets_selected": len(index_infos),
            "excluded_symbols": sorted(excluded_syms) if excluded_syms else [],
            "excluded_count": len(excluded_syms),
        },
        "summary": {
            "passed": passed,
            "failed": failed,
            "errored": errored,
            "top_reasons": reason_counter.most_common(25),
        },
        "results": results,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=False, default=_json_default))
    print(f"Wrote report: {out_path}")
    print(f"PASS={passed} FAIL={failed} ERR={errored} (index_parquets={len(index_infos)})")
    if reason_counter:
        print("Top fail reasons:")
        for r, c in reason_counter.most_common(10):
            print(f"  {c:4d}  {r}")

    # Write minimal gate artifacts (pass/fail/err symbol lists) for downstream tools.
    # These are required even when the sweep runs in safe_mode (no model packaging).
    artifacts_dir = Path(cfg.paths.reports_dir) / "cascade" / run_id
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    pass_syms = sorted({(r.get("symbol") or "").strip().upper() for r in results if r.get("passed")})
    err_syms = sorted({(r.get("symbol") or "").strip().upper() for r in results if r.get("error")})
    fail_syms = sorted(
        {
            (r.get("symbol") or "").strip().upper()
            for r in results
            if (not r.get("passed")) and (not r.get("error"))
        }
    )
    # drop empties
    pass_syms = [s for s in pass_syms if s]
    fail_syms = [s for s in fail_syms if s]
    err_syms = [s for s in err_syms if s]
    (artifacts_dir / "pass_symbols_1D.txt").write_text(("\n".join(pass_syms) + "\n") if pass_syms else "")
    (artifacts_dir / "fail_symbols_1D.txt").write_text(("\n".join(fail_syms) + "\n") if fail_syms else "")
    (artifacts_dir / "err_symbols_1D.txt").write_text(("\n".join(err_syms) + "\n") if err_syms else "")

    # Build and write gate manifest for this run (single source of truth)
    try:
        import hashlib

        # dataset fingerprint: hash of discovered parquet paths + mtime + size
        def make_dataset_fp(items: List[ParquetFileInfo]) -> str:
            arr = []
            for it in sorted(items, key=lambda x: x.path.as_posix()):
                try:
                    st = it.path.stat()
                    arr.append(f"{it.path.as_posix()}|{int(st.st_mtime)}|{st.st_size}")
                except Exception:
                    arr.append(str(it.path.as_posix()))
            s = "\n".join(arr)
            return hashlib.sha256(s.encode('utf-8')).hexdigest()

        dataset_fp = make_dataset_fp(discovered)
        universe_syms = [p.symbol for p in index_infos_all]
        universe_fp = hashlib.sha256("\n".join(sorted(universe_syms)).encode('utf-8')).hexdigest()
        # config fingerprint: use the serialized TrainingConfig to match runtime check
        try:
            cfg_raw = cfg.model_dump() if hasattr(cfg, 'model_dump') else (cfg.dict() if hasattr(cfg, 'dict') else {})
            cfg_fp = hashlib.sha256(json.dumps(cfg_raw, sort_keys=True, default=_json_default).encode('utf-8')).hexdigest()
        except Exception:
            cfg_fp = hashlib.sha256(json.dumps(payload.get('config', {}), sort_keys=True, default=_json_default).encode('utf-8')).hexdigest()

        # infer gate version from results if present
        gate_versions = [ (r.get('gate') or {}).get('gate_version') for r in payload.get('results',[]) if (r.get('gate') or {}).get('gate_version')]
        gate_version = gate_versions[0] if gate_versions else None

        manifest = {
            "run_id": run_id,
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "gate_version": gate_version,
            "asset_class": "indices",
            "timeframes": ["1D"],
            "artifacts_dir": str(artifacts_dir),
            "pass_files": {"1D": "pass_symbols_1D.txt"},
            "fail_files": {"1D": "fail_symbols_1D.txt"},
            "err_files": {"1D": "err_symbols_1D.txt"},
            "exclude_symbols_file": str(args.exclude_symbols_file) if args.exclude_symbols_file else None,
            "dataset_fingerprint": dataset_fp,
            "symbol_universe_fingerprint": universe_fp,
            "config_fingerprint": cfg_fp,
            "manifest_dir": str(Path(cfg.paths.reports_dir) / "gates" / run_id),
            "reports_dir": str(Path(cfg.paths.reports_dir)),
            "git_commit": None,
            "notes": "fail-closed",
        }

        manifest_dir = Path(manifest["manifest_dir"])
        manifest_dir.mkdir(parents=True, exist_ok=True)
        (manifest_dir / "gate_manifest.json").write_text(json.dumps(manifest, indent=2, default=_json_default))
        print(f"Wrote gate manifest: {manifest_dir / 'gate_manifest.json'}")
    except Exception as e:
        print(f"Failed to write gate manifest: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
